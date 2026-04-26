from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

import pytest

from processors.vehicle_deviation import ValidationError, VehicleDeviationProcessor
from processors.vehicle_position import VehiclePositionProcessor
from route_store import RoutePlan


class FakeRouteStore:
    def __init__(self, route_plan=None):
        self.route_plan = route_plan

    def load_route_plan(self, vehicle_id, job_id):
        _ = (vehicle_id, job_id)
        return self.route_plan


def make_route_plan():
    return RoutePlan(
        route_plan_id="11111111-1111-1111-1111-111111111111",
        vehicle_id="LORRY-01",
        job_id="22222222-2222-2222-2222-222222222222",
        waypoints=[
            {"latitude": 6.9271, "longitude": 79.8612},
            {"latitude": 6.9280, "longitude": 79.8620},
            {"latitude": 6.9290, "longitude": 79.8630},
        ],
    )


def make_processor(route_plan=None):
    return VehicleDeviationProcessor(FakeRouteStore(route_plan or make_route_plan()))


def make_gps_message(**overrides):
    message = {
        "vehicle_id": "LORRY-01",
        "latitude": 6.9271,
        "longitude": 79.8612,
        "timestamp": "2026-04-22T10:00:00Z",
        "job_id": "22222222-2222-2222-2222-222222222222",
    }
    message.update(overrides)
    return message


def test_valid_gps_event_parses_with_existing_parser():
    processor = VehiclePositionProcessor()
    parsed = processor.parse_kafka_message(make_gps_message())

    assert parsed["vehicle_id"] == "LORRY-01"
    assert parsed["job_id"] == "22222222-2222-2222-2222-222222222222"
    assert parsed["timestamp"] == datetime(2026, 4, 22, 10, 0, tzinfo=timezone.utc)


def test_invalid_gps_event_rejected():
    processor = VehicleDeviationProcessor(FakeRouteStore(make_route_plan()))

    with pytest.raises(ValidationError, match="latitude must be between -90 and 90"):
        processor.parse_gps_event(make_gps_message(latitude=120.0))


def test_haversine_distance_calculation():
    processor = make_processor()

    distance_m = processor.haversine(0.0, 0.0, 0.0, 1.0)

    assert 111000 <= distance_m <= 112500


def test_nearest_waypoint_distance():
    processor = make_processor()
    route_plan = make_route_plan()
    gps_event = {
        "vehicle_id": "LORRY-01",
        "latitude": 6.92712,
        "longitude": 79.86122,
        "timestamp": datetime(2026, 4, 22, 10, 0, tzinfo=timezone.utc),
        "job_id": "22222222-2222-2222-2222-222222222222",
    }

    distance_m = processor.nearest_waypoint_distance(gps_event, route_plan)

    assert distance_m < 5


def test_no_deviation_when_vehicle_near_route():
    processor = make_processor()

    alert = processor.process(make_gps_message())

    assert alert is None
    assert processor._state == {}


def test_deviation_starts_when_distance_exceeds_threshold():
    processor = make_processor()
    far_message = make_gps_message(latitude=6.99, longitude=79.95)

    alert = processor.process(far_message)

    assert alert is None
    assert processor._state[("LORRY-01", "22222222-2222-2222-2222-222222222222")].alert_sent is False


def test_alert_generated_after_more_than_two_minutes_deviation():
    processor = make_processor()
    first = make_gps_message(latitude=6.99, longitude=79.95, timestamp="2026-04-22T10:00:00Z")
    second = make_gps_message(latitude=6.99, longitude=79.95, timestamp="2026-04-22T10:02:01Z")

    assert processor.process(first) is None
    alert = processor.process(second)

    assert alert is not None
    assert alert["vehicle_id"] == "LORRY-01"
    assert alert["job_id"] == "22222222-2222-2222-2222-222222222222"
    assert alert["duration_s"] > 120
    assert alert["deviation_m"] > 500


def test_state_resets_when_vehicle_returns_to_route():
    processor = make_processor()
    far_one = make_gps_message(latitude=6.99, longitude=79.95, timestamp="2026-04-22T10:00:00Z")
    near = make_gps_message(latitude=6.9271, longitude=79.8612, timestamp="2026-04-22T10:01:00Z")
    far_two = make_gps_message(latitude=6.99, longitude=79.95, timestamp="2026-04-22T10:03:30Z")

    assert processor.process(far_one) is None
    assert processor.process(near) is None
    assert processor._state == {}
    assert processor.process(far_two) is None
    assert processor._state[("LORRY-01", "22222222-2222-2222-2222-222222222222")].start_time == datetime(
        2026, 4, 22, 10, 3, 30, tzinfo=timezone.utc
    )