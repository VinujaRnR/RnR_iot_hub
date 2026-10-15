"""
Tests for Schedule Normalizer.
Verifies Step 5 requirement:
- Supplied production schedule sample produces exactly 16 unique executable actions
- Empty second schedule produces none (0 actions)
- Conflict detection and resolution policies
"""

import pytest
from ingestion.schedule_normalizer import (
    ScheduleNormalizer,
    NormalizedActionRecord,
    ConflictPolicy,
    ScheduleConflictError,
    normalize_schedule,
)


@pytest.fixture
def sample_production_schedule():
    """
    Supplied production schedule sample containing 16 timed actions for
    production_order 174513, resource 20020, sub_resource 20023, device 98a316e6b658.
    """
    # 16 alternating lighting actions across the work shift
    times = [
        "08:00:00", "09:56:00", "10:15:00", "11:00:00",
        "12:00:00", "12:45:00", "13:30:00", "14:15:00",
        "15:00:00", "15:30:00", "16:00:00", "16:30:00",
        "17:00:00", "17:30:00", "18:00:00", "18:30:00",
    ]
    actions_list = []
    for idx, t in enumerate(times, start=1):
        action_name = "LIGHTS_ON" if idx % 2 != 0 else "LIGHTS_OFF"
        if idx == 2:
            action_name = "LIGHTS_ON"  # Matches user example: action-001 or 09:56:00 LIGHTS_ON
        actions_list.append(
            {
                "action_id": f"action-{idx:03d}",
                "action": action_name,
                "time": t,
                "channel_key": "relay:light",
                "iot_node_id": "98a316e6b658",
                "stage_id": 30,
                "params": {},
            }
        )

    return {
        "schedule_id": "hub-01-2026-10-09",
        "hub_id": "hub-01",
        "date": "2026-10-09",
        "timezone": "+05:30",
        "production_orders": [
            {
                "production_order_id": 174513,
                "stage_id": 30,
                "resources": [
                    {
                        "resource_id": 20020,
                        "sub_resources": [
                            {
                                "sub_resource_id": 20023,
                                "iot_node_id": "98a316e6b658",
                                "channel_key": "relay:light",
                                "actions": actions_list,
                            }
                        ],
                    }
                ],
            }
        ],
    }


@pytest.fixture
def empty_second_schedule():
    """An empty schedule with no timed actions."""
    return {
        "schedule_id": "hub-01-2026-10-09-empty",
        "hub_id": "hub-01",
        "date": "2026-10-09",
        "timezone": "Asia/Kolkata",
        "production_orders": [
            {
                "production_order_id": 174599,
                "resources": [
                    {
                        "resource_id": 20099,
                        "sub_resources": [
                            {
                                "sub_resource_id": 20099,
                                "actions": [],
                            }
                        ],
                    }
                ],
            }
        ],
    }


def test_sample_produces_16_unique_executable_actions(sample_production_schedule):
    """Step 5 requirement: The supplied sample produces 16 unique executable actions."""
    records = normalize_schedule(sample_production_schedule)
    assert len(records) == 16, f"Expected 16 actions, got {len(records)}"

    # Check that all records are valid NormalizedActionRecord objects
    for r in records:
        assert isinstance(r, NormalizedActionRecord)
        assert r.schedule_id == "hub-01-2026-10-09"
        assert r.production_order_id == 174513
        assert r.resource_id == 20020
        assert r.sub_resource_id == 20023
        assert r.iot_node_id == "98a316e6b658"
        assert r.channel_key == "relay:light"
        assert r.stage_id == 30
        assert r.params in ({}, None)

    # Verify matching record for 09:56:00
    action_0956 = next(r for r in records if "09:56:00" in r.scheduled_at)
    assert action_0956.scheduled_at == "2026-10-09T09:56:00+05:30"
    assert action_0956.action == "LIGHTS_ON"
    assert action_0956.channel_key == "relay:light"
    assert action_0956.iot_node_id == "98a316e6b658"


def test_empty_second_schedule_produces_none(empty_second_schedule):
    """Step 5 requirement: The empty second schedule produces none."""
    records = normalize_schedule(empty_second_schedule)
    assert len(records) == 0, f"Expected 0 actions, got {len(records)}"
    assert records == []


def test_deduplication_of_identical_actions():
    """Identical duplicate actions targeting same relay at the same time are de-duplicated."""
    schedule = {
        "schedule_id": "sched-dedup",
        "date": "2026-10-09",
        "timezone": "+05:30",
        "production_orders": [
            {
                "production_order_id": 100,
                "resources": [
                    {
                        "resource_id": 200,
                        "sub_resources": [
                            {
                                "sub_resource_id": 300,
                                "actions": [
                                    {
                                        "action_id": "act-1",
                                        "action": "LIGHTS_ON",
                                        "time": "10:00:00",
                                        "channel_key": "relay:1",
                                        "iot_node_id": "node_01",
                                    },
                                    {
                                        "action_id": "act-2",
                                        "action": "LIGHTS_ON",
                                        "time": "10:00:00",
                                        "channel_key": "relay:1",
                                        "iot_node_id": "node_01",
                                    },
                                ],
                            }
                        ],
                    }
                ],
            }
        ],
    }

    records = normalize_schedule(schedule)
    # 2 identical actions should de-duplicate to 1
    assert len(records) == 1
    assert records[0].action == "LIGHTS_ON"


def test_conflicting_instructions_rejection_policy():
    """Conflicting instructions at the same time on the same relay are rejected under REJECT policy."""
    schedule = {
        "schedule_id": "sched-conflict",
        "date": "2026-10-09",
        "timezone": "+05:30",
        "production_orders": [
            {
                "production_order_id": 101,
                "resources": [
                    {
                        "resource_id": 201,
                        "sub_resources": [
                            {
                                "sub_resource_id": 301,
                                "actions": [
                                    {
                                        "action": "LIGHTS_ON",
                                        "time": "12:00:00",
                                        "channel_key": "relay:light",
                                        "iot_node_id": "node_relay_01",
                                    },
                                    {
                                        "action": "LIGHTS_OFF",
                                        "time": "12:00:00",
                                        "channel_key": "relay:light",
                                        "iot_node_id": "node_relay_01",
                                    },
                                ],
                            }
                        ],
                    }
                ],
            }
        ],
    }

    with pytest.raises(ScheduleConflictError) as exc:
        normalize_schedule(schedule, conflict_policy=ConflictPolicy.REJECT)

    assert "Conflicting actions" in str(exc.value)
    assert "node_relay_01" in str(exc.value)
    assert "relay:light" in str(exc.value)


def test_conflicting_instructions_priority_safety_policy():
    """Under PRIORITY_SAFETY, safety-off action takes precedence over on."""
    schedule = {
        "schedule_id": "sched-conflict-safety",
        "date": "2026-10-09",
        "timezone": "+05:30",
        "production_orders": [
            {
                "production_order_id": 101,
                "resources": [
                    {
                        "resource_id": 201,
                        "sub_resources": [
                            {
                                "sub_resource_id": 301,
                                "actions": [
                                    {
                                        "action": "LIGHTS_ON",
                                        "time": "12:00:00",
                                        "channel_key": "relay:light",
                                        "iot_node_id": "node_relay_01",
                                    },
                                    {
                                        "action": "LIGHTS_OFF",
                                        "time": "12:00:00",
                                        "channel_key": "relay:light",
                                        "iot_node_id": "node_relay_01",
                                    },
                                ],
                            }
                        ],
                    }
                ],
            }
        ],
    }

    records = normalize_schedule(schedule, conflict_policy=ConflictPolicy.PRIORITY_SAFETY)
    assert len(records) == 1
    assert records[0].action == "LIGHTS_OFF"


def test_load_sample_json_file():
    """Verifies that loading ingestion/sample_production_schedule.json produces 16 unique actions."""
    import json
    from pathlib import Path

    file_path = Path(__file__).resolve().parent.parent / "ingestion" / "sample_production_schedule.json"
    assert file_path.exists(), f"Sample file not found at {file_path}"

    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    records = normalize_schedule(data)
    assert len(records) == 16
    assert records[0].production_order_id == 174513
    assert records[0].resource_id == 20020
    assert records[0].sub_resource_id == 20023
    assert records[0].iot_node_id == "98a316e6b658"
    assert records[0].channel_key == "relay:light"
