"""
Missed-Action Recovery Policies for RnR Linux IoT Hub.
File: scheduler/missed_action_policy.py

Implements Step 12 specification:
- Defines deterministic recovery policies for actions missed during hub downtime/restart.
- DESIRED_STATE: For idempotent stateful channels (e.g. relay:light, switches, valves),
  determines the final state that the system should be in and applies it once, rather than
  replaying intermediate redundant transitions.
- SKIP_UNSAFE: For non-idempotent actions (e.g. motor movement, material dispensing,
  feeder actuation), uncertain or past commands are marked as skipped to prevent dangerous duplication.
- LATEST_ONLY: Executes only the most recent command and skips earlier ones.
- REPLAY_ALL: Sequentially replays all actions (for logging or idempotent telemetry).
"""

from __future__ import annotations
from enum import Enum
from typing import Any, Dict, List, Optional
import logging

logger = logging.getLogger("iothub.missed_action_policy")


class RecoveryPolicy(str, Enum):
    """Policy applied to missed scheduled actions upon hub restart."""
    DESIRED_STATE = "DESIRED_STATE"  # Calculate and apply single target state (e.g. relay ON/OFF)
    SKIP_UNSAFE = "SKIP_UNSAFE"      # Non-idempotent: skip execution and raise alert
    LATEST_ONLY = "LATEST_ONLY"      # Execute the newest action only, skip older ones
    REPLAY_ALL = "REPLAY_ALL"        # Sequentially replay all missed actions


class MissedActionPolicy:
    """
    Classifies actions and determines safe recovery behavior based on channel and action type.
    """

    # Channels that are inherently stateful and idempotent (safe for desired-state recovery)
    DEFAULT_DESIRED_STATE_CHANNELS = {
        "relay:light",
        "relay:pump",
        "relay:fan",
        "relay:heater",
        "relay:valve",
        "relay:solenoid",
        "switch",
        "thermostat",
    }

    # Actions that are non-idempotent and dangerous to repeat blindly after restart
    DEFAULT_NON_IDEMPOTENT_ACTIONS = {
        "dispense",
        "feed",
        "dose",
        "inject",
        "move_motor",
        "step_motor",
        "rotate",
        "trigger_pulse",
    }

    def __init__(
        self,
        desired_state_channels: Optional[set[str]] = None,
        non_idempotent_actions: Optional[set[str]] = None,
    ):
        self.desired_state_channels = (
            desired_state_channels or set(self.DEFAULT_DESIRED_STATE_CHANNELS)
        )
        self.non_idempotent_actions = (
            non_idempotent_actions or set(self.DEFAULT_NON_IDEMPOTENT_ACTIONS)
        )

    def classify_policy(self, channel_key: str, action_name: str) -> RecoveryPolicy:
        """
        Determines the appropriate recovery policy for a given channel and action.
        """
        action_clean = action_name.strip().lower()
        channel_clean = channel_key.strip().lower()

        # 1. Non-idempotent actions must never be repeated blindly
        for non_idempotent in self.non_idempotent_actions:
            if non_idempotent in action_clean:
                return RecoveryPolicy.SKIP_UNSAFE

        # 2. Stateful relay / lighting channels use desired-state recovery
        for ds_channel in self.desired_state_channels:
            if ds_channel in channel_clean or channel_clean.startswith(ds_channel):
                return RecoveryPolicy.DESIRED_STATE

        # 3. Standard lighting commands (LIGHTS_ON / LIGHTS_OFF) are always desired-state
        if action_clean in ("lights_on", "lights_off", "on", "off", "set_led"):
            return RecoveryPolicy.DESIRED_STATE

        # Default safe fallback
        return RecoveryPolicy.SKIP_UNSAFE

    def compute_desired_state(
        self,
        missed_actions: List[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        """
        For a sequence of missed actions on a desired-state channel (e.g. relay:light),
        determines the target action that represents the final intended state.
        
        Example:
          [09:56 LIGHTS_ON, 10:00 LIGHTS_OFF, 10:07 LIGHTS_ON]
          -> Returns the 10:07 LIGHTS_ON action record.
        """
        if not missed_actions:
            return None

        # Sort missed actions chronologically by scheduled_at
        sorted_actions = sorted(missed_actions, key=lambda a: a.get("scheduled_at", ""))
        return sorted_actions[-1]
