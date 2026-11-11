import json
import logging
from typing import Any, Dict, List, Optional
from core.message_validator import IoTMessage
from devices.device_manager import DeviceManager, DeviceNotFoundError, create_default_device_manager
from devices.mock_device import DeviceActionError

logger = logging.getLogger("iothub.router")


class CommandRoutingError(Exception):
    """Raised when routing fails entirely (e.g. hub_id mismatch)."""
    pass


class CommandRouter:
    """Routes validated IoT commands to registered devices."""

    def __init__(
        self,
        hub_id: str,
        device_manager: DeviceManager | None = None,
        registration_guard: Optional[Any] = None,
    ):
        self.hub_id = hub_id
        self.device_manager = device_manager or create_default_device_manager()
        self.registration_guard = registration_guard

    async def route_message(self, message: IoTMessage) -> List[Dict[str, Any]]:
        """
        Routes all commands in a validated message to their target devices.
        Returns a list of execution results for each command.
        """
        if message.hub_id != self.hub_id:
            raise CommandRoutingError(
                f"Hub ID mismatch: message destination '{message.hub_id}' != local hub '{self.hub_id}'"
            )

        if self.registration_guard:
            authorized, auth_reason = await self.registration_guard.check_hub_authorized()
            if not authorized:
                logger.warning("[GUARD] Inbound message %s rejected: %s", message.message_id, auth_reason)
                raise CommandRoutingError(f"Hub unauthorized: {auth_reason}")

        results: List[Dict[str, Any]] = []
        logger.info("Routing message %s with %d command(s)", message.message_id, len(message.commands))

        for cmd in message.commands:
            cmd_result = {
                "command_id": cmd.command_id,
                "device_id": cmd.device_id,
                "action": cmd.action,
            }

            # Device authorization check via registration guard
            if self.registration_guard:
                device_approved = await self.registration_guard.is_device_approved(cmd.device_id)
                if not device_approved:
                    cmd_result["status"] = "error"
                    cmd_result["error"] = "DEVICE_NOT_AUTHORIZED"
                    cmd_result["message"] = f"Device '{cmd.device_id}' is not authorized by Django middleware inventory."
                    logger.warning("[GUARD] Rejected command on unauthorized device '%s'", cmd.device_id)
                    results.append(cmd_result)
                    continue

            params_str = json.dumps(cmd.parameters)
            logger.info("Target=%s | Action=%s | Params=%s", cmd.device_id, cmd.action, params_str)

            try:
                device_response = await self.device_manager.dispatch(
                    device_id=cmd.device_id,
                    action=cmd.action,
                    parameters=cmd.parameters,
                )
                cmd_result["status"] = "success"
                cmd_result["result"] = device_response.get("result") if "result" in device_response else device_response
                logger.info("Executed %s on %s: %s", cmd.action, cmd.device_id, cmd_result["result"])
            except DeviceNotFoundError as err:
                cmd_result["status"] = "error"
                cmd_result["error"] = "DEVICE_NOT_FOUND"
                cmd_result["message"] = str(err)
                logger.error("Device not found: %s", err)
            except DeviceActionError as err:
                cmd_result["status"] = "error"
                cmd_result["error"] = "ACTION_ERROR"
                cmd_result["message"] = str(err)
                logger.error("Action error on device '%s': %s", cmd.device_id, err)
            except Exception as err:
                cmd_result["status"] = "error"
                cmd_result["error"] = "EXECUTION_FAILURE"
                cmd_result["message"] = str(err)
                logger.exception("Unexpected execution error for command %s: %s", cmd.command_id, err)

            results.append(cmd_result)

        return results
