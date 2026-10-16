import pytest
from ingestion.schemas import (
    DailyProductionSchedule,
    ScheduleEntry,
    ProductionOrder,
    Resource,
    SubResource,
    ScheduledAction,
)
from ingestion.schedule_validator import (
    ScheduleValidator,
    ScheduleValidationError,
    MalformedScheduleError,
    ScheduleRevisionError,
    UnauthorizedActionError,
    UnknownDeviceError,
    InvalidChannelError,
    SubResourceMismatchError,
    ScheduleRevisionTracker,
    validate_daily_schedule,
)


@pytest.fixture
def validator():
    tracker = ScheduleRevisionTracker()
    return ScheduleValidator(
        expected_hub_id="hub-01",
        known_devices={"iot_001", "iot_002"},
        device_channels={
            "iot_001": {"led", "ch1", "main"},
            "iot_002": {"temp", "humidity", "sensor", "ch1"},
        },
        revision_tracker=tracker,
    )


def test_valid_schedule_via_schedules_array(validator):
    payload = {
        "schema_version": 1,
        "schedule_id": "sched_20261012_01",
        "hub_id": "hub-01",
        "revision": 1,
        "date": "2026-10-12",
        "timezone": "Asia/Kolkata",
        "schedules": [
            {
                "time": "08:30",
                "action": "set_led",
                "channel_key": "led",
                "params": {"state": "on"},
                "sub_resource": "sub_assembly_1",
                "iot_node_id": "iot_001",
                "production_order_id": "PO-100",
                "resource_id": "LINE-1",
            },
            {
                "time": "17:45",
                "action": "read_temperature",
                "channel_key": "temp",
                "params": {"unit": "C"},
                "sub_resource": "sub_assembly_2",
                "iot_node_id": "iot_002",
            },
        ],
    }

    schedule = validator.validate(payload)
    assert schedule.schedule_id == "sched_20261012_01"
    assert schedule.revision == 1
    actions = schedule.extract_all_actions()
    assert len(actions) == 2
    assert actions[0].time == "08:30"
    assert actions[1].action == "read_temperature"


def test_valid_schedule_via_production_orders(validator):
    payload = {
        "schema_version": 1,
        "schedule_id": "sched_po_hierarchical",
        "hub_id": "hub-01",
        "revision": 1,
        "date": "2026-10-12",
        "timezone": "Asia/Kolkata",
        "production_orders": [
            {
                "production_order_id": "PO-2026-55",
                "resources": [
                    {
                        "resource_id": "MACHINE-ALPHA",
                        "sub_resources": [
                            {
                                "sub_resource_id": "SUB-ROBOT-ARM",
                                "actions": [
                                    {
                                        "time": "09:00",
                                        "action": "LIGHTS_ON",
                                        "channel_key": "main",
                                        "params": {"intensity": 100},
                                        "sub_resource": "SUB-ROBOT-ARM",
                                        "iot_node_id": "iot_001",
                                    }
                                ],
                            }
                        ],
                    }
                ],
            }
        ],
    }

    schedule = validator.validate(payload)
    actions = schedule.extract_all_actions()
    assert len(actions) == 1
    assert actions[0].sub_resource == "SUB-ROBOT-ARM"
    assert actions[0].action == "LIGHTS_ON"


def test_invalid_time_format(validator):
    invalid_times = ["25:00", "8:30", "12:60", "noon", "24:00"]
    for bad_time in invalid_times:
        payload = {
            "schema_version": 1,
            "schedule_id": f"sched_time_test_{bad_time}",
            "hub_id": "hub-01",
            "revision": 1,
            "date": "2026-10-12",
            "timezone": "Asia/Kolkata",
            "schedules": [
                {
                    "time": bad_time,
                    "action": "set_led",
                    "channel_key": "led",
                    "params": {},
                    "sub_resource": "sub1",
                    "iot_node_id": "iot_001",
                }
            ],
        }
        with pytest.raises(MalformedScheduleError):
            validator.validate(payload)


def test_unauthorized_action(validator):
    payload = {
        "schema_version": 1,
        "schedule_id": "sched_unauth_action",
        "hub_id": "hub-01",
        "revision": 1,
        "date": "2026-10-12",
        "timezone": "Asia/Kolkata",
        "schedules": [
            {
                "time": "10:00",
                "action": "FORMAT_HARD_DRIVE",
                "channel_key": "led",
                "params": {},
                "sub_resource": "sub1",
                "iot_node_id": "iot_001",
            }
        ],
    }
    with pytest.raises(UnauthorizedActionError) as exc:
        validator.validate(payload)
    assert "unauthorized operation" in str(exc.value)


def test_unknown_device_node_id(validator):
    payload = {
        "schema_version": 1,
        "schedule_id": "sched_unknown_device",
        "hub_id": "hub-01",
        "revision": 1,
        "date": "2026-10-12",
        "timezone": "Asia/Kolkata",
        "schedules": [
            {
                "time": "10:00",
                "action": "set_led",
                "channel_key": "led",
                "params": {},
                "sub_resource": "sub1",
                "iot_node_id": "non_existent_node_999",
            }
        ],
    }
    with pytest.raises(UnknownDeviceError) as exc:
        validator.validate(payload)
    assert "unknown device node identifier" in str(exc.value)


def test_unsupported_channel_key(validator):
    payload = {
        "schema_version": 1,
        "schedule_id": "sched_unsupported_chan",
        "hub_id": "hub-01",
        "revision": 1,
        "date": "2026-10-12",
        "timezone": "Asia/Kolkata",
        "schedules": [
            {
                "time": "10:00",
                "action": "set_led",
                "channel_key": "unsupported_channel_xyz",
                "params": {},
                "sub_resource": "sub1",
                "iot_node_id": "iot_001",
            }
        ],
    }
    with pytest.raises(InvalidChannelError) as exc:
        validator.validate(payload)
    assert "not supported by device" in str(exc.value)


def test_sub_resource_mismatch(validator):
    payload = {
        "schema_version": 1,
        "schedule_id": "sched_sub_mismatch",
        "hub_id": "hub-01",
        "revision": 1,
        "date": "2026-10-12",
        "timezone": "Asia/Kolkata",
        "production_orders": [
            {
                "production_order_id": "PO-1",
                "resources": [
                    {
                        "resource_id": "RES-1",
                        "sub_resources": [
                            {
                                "sub_resource_id": "SUB-RESOURCE-A",
                                "actions": [
                                    {
                                        "time": "10:00",
                                        "action": "set_led",
                                        "channel_key": "led",
                                        "params": {},
                                        "sub_resource": "SUB-RESOURCE-DIFFERENT-B",
                                        "iot_node_id": "iot_001",
                                    }
                                ],
                            }
                        ],
                    }
                ],
            }
        ],
    }
    with pytest.raises(SubResourceMismatchError) as exc:
        validator.validate(payload)
    assert "sub_resource mismatch" in str(exc.value)


def test_hub_id_mismatch(validator):
    payload = {
        "schema_version": 1,
        "schedule_id": "sched_hub_mismatch",
        "hub_id": "other-hub-remote",
        "revision": 1,
        "date": "2026-10-12",
        "timezone": "Asia/Kolkata",
        "schedules": [
            {
                "time": "10:00",
                "action": "set_led",
                "channel_key": "led",
                "params": {},
                "sub_resource": "sub1",
                "iot_node_id": "iot_001",
            }
        ],
    }
    with pytest.raises(ScheduleValidationError) as exc:
        validator.validate(payload)
    assert "Hub ID mismatch" in str(exc.value)


def test_revision_tracking_and_deduplication(validator):
    payload_rev1 = {
        "schema_version": 1,
        "schedule_id": "sched_rev_test",
        "hub_id": "hub-01",
        "revision": 1,
        "date": "2026-10-12",
        "timezone": "Asia/Kolkata",
        "schedules": [
            {
                "time": "10:00",
                "action": "set_led",
                "channel_key": "led",
                "params": {},
                "sub_resource": "sub1",
                "iot_node_id": "iot_001",
            }
        ],
    }

    # First attempt: succeeds
    res1 = validator.validate(payload_rev1)
    assert res1.revision == 1

    # Second attempt with same revision: rejected as duplicate
    with pytest.raises(ScheduleRevisionError) as exc:
        validator.validate(payload_rev1)
    assert "Stale or duplicate schedule" in str(exc.value)

    # Attempt with older revision: rejected
    payload_rev0 = dict(payload_rev1, revision=0)
    with pytest.raises(ScheduleRevisionError):
        validator.validate(payload_rev0)

    # Attempt with newer revision (rev 2): succeeds
    payload_rev2 = dict(payload_rev1, revision=2)
    res2 = validator.validate(payload_rev2)
    assert res2.revision == 2


def test_invalid_date_and_timezone(validator):
    base_payload = {
        "schema_version": 1,
        "schedule_id": "sched_dt_test",
        "hub_id": "hub-01",
        "revision": 1,
        "date": "2026-10-12",
        "timezone": "Asia/Kolkata",
        "schedules": [
            {
                "time": "10:00",
                "action": "set_led",
                "channel_key": "led",
                "params": {},
                "sub_resource": "sub1",
                "iot_node_id": "iot_001",
            }
        ],
    }

    # Invalid calendar date
    bad_date = dict(base_payload, date="2026-02-31")
    with pytest.raises(MalformedScheduleError):
        validator.validate(bad_date)

    # Invalid timezone string
    bad_tz = dict(base_payload, timezone="Mars/Phobos")
    with pytest.raises(MalformedScheduleError):
        validator.validate(bad_tz)


def test_malformed_json_bytes_and_string(validator):
    with pytest.raises(MalformedScheduleError):
        validator.validate(b"INVALID_JSON_{{<<>>")

    with pytest.raises(MalformedScheduleError):
        validator.validate("NON_JSON_STRING")

    with pytest.raises(MalformedScheduleError):
        validator.validate(["not_a_dict"])


def test_empty_schedule_rejected(validator):
    payload = {
        "schema_version": 1,
        "schedule_id": "sched_empty",
        "hub_id": "hub-01",
        "revision": 1,
        "date": "2026-10-12",
        "timezone": "Asia/Kolkata",
        "schedules": [],
        "production_orders": [],
    }
    with pytest.raises(MalformedScheduleError) as exc:
        validator.validate(payload)
    assert "must contain at least one scheduled action" in str(exc.value)


def test_revision_content_change_allowed(validator):
    payload_original = {
        "schema_version": 1,
        "schedule_id": "sched_update_test",
        "hub_id": "hub-01",
        "revision": 1,
        "date": "2026-10-12",
        "timezone": "Asia/Kolkata",
        "schedules": [
            {
                "time": "10:00",
                "action": "set_led",
                "channel_key": "led",
                "params": {},
                "sub_resource": "sub1",
                "iot_node_id": "iot_001",
            }
        ],
    }

    # First validation succeeds
    res1 = validator.validate(payload_original)
    assert res1.schedule_id == "sched_update_test"

    # Same revision, identical content -> rejected as duplicate
    with pytest.raises(ScheduleRevisionError):
        validator.validate(payload_original)

    # Same revision, modified content (action time changed) -> accepted as modified update!
    payload_modified = {
        "schema_version": 1,
        "schedule_id": "sched_update_test",
        "hub_id": "hub-01",
        "revision": 1,
        "date": "2026-10-12",
        "timezone": "Asia/Kolkata",
        "schedules": [
            {
                "time": "11:30",
                "action": "set_led",
                "channel_key": "led",
                "params": {},
                "sub_resource": "sub1",
                "iot_node_id": "iot_001",
            }
        ],
    }
    res2 = validator.validate(payload_modified)
    assert res2.schedule_id == "sched_update_test"
    actions = res2.extract_all_actions()
    assert actions[0].time == "11:30"

