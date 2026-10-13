"""
State Reconciler for RnR Linux IoT Hub.
File: scheduler/state_reconciler.py

Implements Step 12 specification:
- Executes on hub startup/restart to reconcile missed actions with physical hardware.
- Implements desired-state recovery for relay/lighting actions (establishes target state
  without replaying intermediate state transitions).
- Enforces strict safety for non-idempotent actions (skips missed motor/dispensing commands
  and generates alerts rather than repeating them blindly).
- Future actions remain untouched in PENDING state to execute at their designated times.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import logging
from typing import Any, Dict, List, Optional

from storage.database import DatabaseManager
from storage.schedule_repository import ScheduleRepository
from devices.device_manager import DeviceManager
from scheduler.missed_action_policy import MissedActionPolicy, RecoveryPolicy

logger = logging.getLogger("iothub.state_reconciler")


@dataclass
class ReconciliationReport:
    """Summary of state reconciliation operations performed upon startup."""
    reconciled_at: str
    schedule_id: Optional[str] = None
    applied_actions: List[Dict[str, Any]] = field(default_factory=list)
    superseded_actions: List[str] = field(default_factory=list)
    skipped_unsafe_actions: List[str] = field(default_factory=list)
    future_pending_count: int = 0
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "reconciled_at": self.reconciled_at,
            "schedule_id": self.schedule_id,
            "applied_count": len(self.applied_actions),
            "superseded_count": len(self.superseded_actions),
            "skipped_unsafe_count": len(self.skipped_unsafe_actions),
            "future_pending_count": self.future_pending_count,
            "applied_actions": self.applied_actions,
            "superseded_actions": self.superseded_actions,
            "skipped_unsafe_actions": self.skipped_unsafe_actions,
            "errors": self.errors,
        }


class StateReconciler:
    """
    Reconciles stored schedule actions with actual physical device state upon startup.
    Guarantees that hub restarts do not cause duplicate or unsafe physical actions.
    """

    def __init__(
        self,
        db: DatabaseManager,
        schedule_repo: ScheduleRepository,
        device_manager: DeviceManager,
        policy: Optional[MissedActionPolicy] = None,
    ):
        self.db = db
        self.schedule_repo = schedule_repo
        self.device_manager = device_manager
        self.policy = policy or MissedActionPolicy()

    async def reconcile_on_startup(
        self,
        current_time_iso: Optional[str] = None,
    ) -> ReconciliationReport:
        """
        Main entry point invoked when the IoT Hub starts or recovers from a reboot.
        
        1. Loads the latest active schedule and associated actions from SQLite.
        2. Segregates past missed actions from future pending actions.
        3. For stateful channels (relay:light), computes and applies the single desired state.
        4. For non-idempotent actions (motors/dispensers), skips them safely and logs alerts.
        5. Preserves all future scheduled actions for normal runtime execution.
        """
        now_utc = datetime.now(timezone.utc)
        if current_time_iso:
            try:
                now_utc = datetime.fromisoformat(current_time_iso).astimezone(timezone.utc)
            except Exception:
                pass

        now_iso = now_utc.isoformat()
        report = ReconciliationReport(reconciled_at=now_iso)

        # 1. Fetch active schedules from SQLite
        schedules = await self.schedule_repo.list_schedules(status="ACTIVE")
        if not schedules:
            logger.info("[RECONCILER] No active schedules found in database to reconcile.")
            return report

        active_schedule = schedules[0]
        schedule_id = active_schedule["schedule_id"]
        report.schedule_id = schedule_id

        # 2. Fetch all scheduled actions for this schedule
        all_actions = await self.schedule_repo.get_scheduled_actions(schedule_id)
        if not all_actions:
            logger.info("[RECONCILER] Schedule '%s' has no stored actions.", schedule_id)
            return report

        # 3. Partition actions into:
        #    - missed (past due and not yet COMPLETED)
        #    - future (scheduled after now)
        missed_actions: List[Dict[str, Any]] = []
        future_count = 0

        for act in all_actions:
            status = act.get("status", "PENDING")
            sched_str = act.get("scheduled_at", "")
            try:
                sched_dt = datetime.fromisoformat(sched_str).astimezone(timezone.utc)
            except Exception:
                continue

            if sched_dt <= now_utc:
                # Action was due in the past. If not definitively completed, it was interrupted/missed
                if status in ("PENDING", "CLAIMED", "EXECUTING", "IN_FLIGHT"):
                    missed_actions.append(act)
            else:
                # Future scheduled action
                if status in ("CLAIMED", "EXECUTING", "IN_FLIGHT"):
                    # Was in-flight before reboot; reset to PENDING so it executes when due
                    await self.schedule_repo.update_action_status(schedule_id, act["action_id"], "PENDING")
                    future_count += 1
                elif status == "PENDING":
                    future_count += 1

        report.future_pending_count = future_count

        if not missed_actions:
            logger.info(
                "[RECONCILER] Zero missed actions found for schedule '%s'. %d future action(s) awaiting execution.",
                schedule_id,
                future_count,
            )
            return report

        logger.info(
            "[RECONCILER] Found %d missed action(s) for schedule '%s'. Beginning policy-based state reconciliation...",
            len(missed_actions),
            schedule_id,
        )

        # 4. Group missed actions by target channel: (iot_node_id, channel_key)
        channel_groups: Dict[tuple[str, str], List[Dict[str, Any]]] = {}
        for act in missed_actions:
            key = (str(act.get("iot_node_id")), str(act.get("channel_key")))
            channel_groups.setdefault(key, []).append(act)

        # 5. Process each channel according to its recovery policy
        for (node_id, channel_key), actions_list in channel_groups.items():
            # Classify channel using policy
            sample_action = actions_list[-1].get("action", "")
            channel_policy = self.policy.classify_policy(channel_key, sample_action)

            if channel_policy == RecoveryPolicy.DESIRED_STATE:
                await self._reconcile_desired_state(schedule_id, node_id, channel_key, actions_list, report)
            elif channel_policy == RecoveryPolicy.SKIP_UNSAFE:
                await self._reconcile_skip_unsafe(schedule_id, node_id, channel_key, actions_list, report)
            else:
                # Default safe handling: skip
                await self._reconcile_skip_unsafe(schedule_id, node_id, channel_key, actions_list, report)

        logger.info(
            "[RECONCILER] State reconciliation complete: %d applied, %d superseded, %d skipped, %d future pending.",
            len(report.applied_actions),
            len(report.superseded_actions),
            len(report.skipped_unsafe_actions),
            report.future_pending_count,
        )
        return report

    async def _reconcile_desired_state(
        self,
        schedule_id: str,
        node_id: str,
        channel_key: str,
        actions_list: List[Dict[str, Any]],
        report: ReconciliationReport,
    ) -> None:
        """
        Applies desired-state recovery for stateful channels (e.g. relay:light).
        Calculates the latest target state, marks prior intermediate actions as SUPERSEDED,
        and applies the single target state to the hardware once.
        """
        # Determine the target desired state action (chronologically latest)
        desired_action = self.policy.compute_desired_state(actions_list)
        if not desired_action:
            return

        desired_id = desired_action["action_id"]
        desired_cmd = desired_action["action"]
        desired_params = desired_action.get("params") or {}

        # 1. Mark all older intermediate actions as SUPERSEDED in SQLite
        for act in actions_list:
            act_id = act["action_id"]
            if act_id != desired_id:
                await self.schedule_repo.update_action_status(
                    schedule_id=schedule_id,
                    action_id=act_id,
                    status="SUPERSEDED",
                )
                report.superseded_actions.append(act_id)
                logger.debug(
                    "[RECONCILER] Action '%s' (%s at %s) marked SUPERSEDED",
                    act_id,
                    act.get("action"),
                    act.get("scheduled_at"),
                )

        # 2. Check current hardware state if available to avoid unnecessary transitions
        needs_dispatch = True
        try:
            device = self.device_manager.get_device(node_id)
            if device and hasattr(device, "_state"):
                current_hw_state = device._state.get("led", "").upper()
                expected_state = "ON" if "ON" in desired_cmd.upper() else "OFF"
                if current_hw_state == expected_state:
                    logger.info(
                        "[RECONCILER] Hardware '%s' is already in desired state (%s). Dispatch redundant.",
                        node_id,
                        expected_state,
                    )
                    needs_dispatch = False
        except Exception as exc:
            logger.debug("Could not inspect current device state: %s", exc)

        # 3. Dispatch the single desired state to hardware
        exec_status = "SUCCESS"
        result_data: Dict[str, Any] = {}
        if needs_dispatch:
            logger.info(
                "[RECONCILER] Applying single DESIRED STATE '%s' to '%s' (Channel: %s, Action ID: %s)",
                desired_cmd,
                node_id,
                channel_key,
                desired_id,
            )
            try:
                result_data = await self.device_manager.dispatch(node_id, desired_cmd, desired_params)
                exec_status = "SUCCESS"
            except Exception as exc:
                exec_status = "FAILED"
                err_msg = f"Failed to apply desired state to {node_id}: {exc}"
                report.errors.append(err_msg)
                logger.error("[RECONCILER] %s", err_msg)

        # 4. Record execution in database and update status
        now_str = datetime.now(timezone.utc).isoformat()
        await self.schedule_repo.record_execution(
            schedule_id=schedule_id,
            action_id=desired_id,
            device_id=node_id,
            command_action=desired_cmd,
            attempt=1,
            status=exec_status,
            result=result_data,
        )
        report.applied_actions.append({
            "action_id": desired_id,
            "device_id": node_id,
            "action": desired_cmd,
            "status": exec_status,
        })

    async def _reconcile_skip_unsafe(
        self,
        schedule_id: str,
        node_id: str,
        channel_key: str,
        actions_list: List[Dict[str, Any]],
        report: ReconciliationReport,
    ) -> None:
        """
        Enforces safety for non-idempotent actions (e.g. motors, material dispensers).
        Marks actions as SKIPPED_UNSAFE to prevent dangerous physical repetition.
        """
        now_str = datetime.now(timezone.utc).isoformat()
        for act in actions_list:
            act_id = act["action_id"]
            action_name = act.get("action", "")

            await self.schedule_repo.update_action_status(
                schedule_id=schedule_id,
                action_id=act_id,
                status="SKIPPED_UNSAFE",
                executed_at=now_str,
            )
            report.skipped_unsafe_actions.append(act_id)

            logger.warning(
                "[RECONCILER SAFETY] Skipped non-idempotent action '%s' (%s on channel '%s') to prevent unsafe duplication.",
                act_id,
                action_name,
                channel_key,
            )
