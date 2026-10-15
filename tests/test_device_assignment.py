"""Device-assignment columns (no params stored) and command JSON built from them."""
import json
from unittest.mock import MagicMock, patch

import pytest

from devices.arduino_nano import ArduinoNanoDevice
from ingestion.schedule_normalizer import ScheduleNormalizer
from storage.database import DatabaseManager
from storage.schedule_repository import ScheduleRepository
from scheduler.action_dispatcher import ActionDispatcher
from devices.device_manager import create_default_device_manager


def _payload():
    action = {
        "action": "LIGHT_ON",
        "params": {},
        "channel_key": "relay:light_glow",
        "sub_resource": 20028,
        "device_type": "light_glow",
        "device_number": 1,
        "device_id": "light_glow_1",
        "device_name": "light_glow #1",
    }
    return {
        "date": "2026-10-15",
        "timezone": "Asia/Kolkata",
        "device_id": "3417ebc3b479",
        "production_order": {"id": 175817, "start_date": "2026-10-14"},
        "resource": {"id": 20026},
        "sub_resources": [
            {
                "id": 20028,
                "iot_node_id": "3417ebc3b479",
                "actions": [
                    {**action, "time": "11:40", "stage_id": 64},
                    {**action, "action": "LIGHT_OFF", "time": "11:45", "stage_id": 65},
                ],
            }
        ],
    }


def test_normalizer_assigns_device_columns():
    recs = ScheduleNormalizer().normalize(_payload())
    assert len(recs) == 2
    for r in recs:
        assert r.iot_node_id == "light_glow_1"
        assert r.device_id == "light_glow_1"
        assert r.device_type == "light_glow"
        assert r.device_number == 1
        assert r.device_name == "light_glow #1"


@pytest.mark.asyncio
async def test_db_has_no_params_columns_and_command_json(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "t.db"))
    await db.connect()
    repo = ScheduleRepository(db)
    recs = ScheduleNormalizer().normalize(_payload())
    await repo.save_schedule_with_actions("s1", "2026-10-15", 1, recs, raw_json="{}")

    async with db.connection.execute("PRAGMA table_info(scheduled_actions)") as cur:
        cols = {r[1] for r in await cur.fetchall()}
    assert "params" not in cols and "params_json" not in cols
    assert {"device_id", "device_type", "device_number", "device_name"} <= cols

    nano = ArduinoNanoDevice(port="/dev/ttyUSB0", device_id="light_glow_1", name="light_glow_1")
    acts = await repo.get_scheduled_actions("s1")
    with patch.object(nano, "_ensure_port_open", return_value=MagicMock()):
        a = acts[0]
        res = await nano.execute_command(
            a["action"], {"target_device": a["device_id"], "params": a["params"]}
        )
    assert res["command"] == {
        "action": "LIGHT_ON",
        "params": {"device_type": "light_glow", "device_number": 1},
    }
    assert "topic" not in res["command"]
    await db.close()
