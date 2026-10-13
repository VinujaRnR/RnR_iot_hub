"""
Configurable Daily Schedule Policies for RnR Linux IoT Hub.
File: scheduler/schedule_policies.py

Implements Customization 6 specification:
- Configurable schedule execution policies stored in SQLite configuration.
- Policies determine:
  * Schedule replacement: 'replace' | 'merge' | 'reject'
  * Duplicate delivery: 'ignore' | 'reject'
  * Late actions: 'reconcile' | 'skip' | 'execute'
  * Offline devices: 'defer' | 'fail' | 'expire'
  * Conflicting actions: 'reject' | 'priority'
  * Missing daily plan: 'alert' | 'safe_hold'
- Dynamically evaluated without requiring Python code changes.
"""

from __future__ import annotations
from dataclasses import asdict, dataclass
import logging
from typing import Any, Dict, Optional

from storage.config_repository import ConfigRepository

logger = logging.getLogger("iothub.schedule_policies")


@dataclass
class SchedulePolicyConfig:
    replacement_policy: str = "replace"     # replace | merge | reject
    duplicate_policy: str = "ignore"         # ignore | reject
    late_action_policy: str = "reconcile"   # reconcile | skip | execute
    offline_device_policy: str = "defer"     # defer | fail | expire
    conflict_policy: str = "reject"         # reject | priority
    missing_schedule_policy: str = "alert"   # alert | safe_hold
    late_grace_seconds: int = 300           # 5 minutes default grace window

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class SchedulePolicyManager:
    """Loads and evaluates dynamic schedule policies from ConfigRepository."""

    def __init__(self, config_repo: ConfigRepository):
        self.config_repo = config_repo

    async def get_policies(self) -> SchedulePolicyConfig:
        """Loads active scheduling policies from SQLite config."""
        replacement = await self.config_repo.get_setting("schedule.replacement_policy", "replace")
        duplicate = await self.config_repo.get_setting("schedule.duplicate_policy", "ignore")
        late_action = await self.config_repo.get_setting("schedule.late_action_policy", "reconcile")
        offline_dev = await self.config_repo.get_setting("schedule.offline_device_policy", "defer")
        conflict = await self.config_repo.get_setting("schedule.conflict_policy", "reject")
        missing = await self.config_repo.get_setting("schedule.missing_schedule_policy", "alert")
        grace = await self.config_repo.get_setting("schedule.late_grace_seconds", "300")

        return SchedulePolicyConfig(
            replacement_policy=str(replacement).lower(),
            duplicate_policy=str(duplicate).lower(),
            late_action_policy=str(late_action).lower(),
            offline_device_policy=str(offline_dev).lower(),
            conflict_policy=str(conflict).lower(),
            missing_schedule_policy=str(missing).lower(),
            late_grace_seconds=int(grace) if str(grace).isdigit() else 300,
        )

    async def set_policy(self, policy_key: str, value: str, actor: str = "cli") -> None:
        """Sets an individual scheduling policy."""
        clean_key = policy_key.lower().replace("-", "_")
        valid_keys = {
            "replacement_policy": "schedule.replacement_policy",
            "duplicate_policy": "schedule.duplicate_policy",
            "late_action_policy": "schedule.late_action_policy",
            "conflict_policy": "schedule.conflict_policy",
            "missing_schedule_policy": "schedule.missing_schedule_policy",
        }
        db_key = valid_keys.get(clean_key, f"schedule.{clean_key}")
        await self.config_repo.set_setting(db_key, value.strip().lower(), is_secret=False, actor=actor)
        logger.info("Schedule policy '%s' updated to '%s'", db_key, value)

    def should_ignore_duplicate(self, policy: SchedulePolicyConfig, existing_rev: int, new_rev: int) -> bool:
        """Evaluates whether to ignore a duplicate delivery."""
        if new_rev <= existing_rev:
            return policy.duplicate_policy == "ignore"
        return False
