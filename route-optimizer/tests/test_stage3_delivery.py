from __future__ import annotations

from datetime import datetime, timezone
import unittest

from models import (
    BinCandidate,
    EmergencyOptimizationSnapshot,
    EmergencyTrigger,
    OptimizationPlan,
    RouteStop,
    VehicleProfile,
    VehicleRoutePlan,
)
from service import (
    build_deterministic_job_id,
    build_optimized_route_event,
    persist_optimization_plan,
    publish_optimized_route_event,
)


class _FakeRepository:
    def __init__(self, already_exists: bool = False):
        self.already_exists = already_exists
        self.saved_calls = []

    def route_plan_exists(self, job_id: str) -> bool:
        return self.already_exists

    def save_optimization_plan(self, job_id: str, zone_id: int, route_type: str, routes):
        routes = tuple(routes)
        self.saved_calls.append((job_id, zone_id, route_type, routes))
        return len(routes)


class _FakeProducer:
    def __init__(self):
        self.sent = []
        self.flush_calls = 0

    def send(self, topic, value):
        self.sent.append((topic, value))

    def flush(self):
        self.flush_calls += 1


def _snapshot() -> EmergencyOptimizationSnapshot:
    return EmergencyOptimizationSnapshot(
        trigger=EmergencyTrigger(
            event_id="2026-04-27T08:00:00Z",
            trigger_bin_id="BIN-001",
            zone_id=1,
            urgency_score=90,
            route_type="emergency",
            event_timestamp="2026-04-27T08:00:00Z",
            payload={"bin_id": "BIN-001", "urgency_score": 90},
        ),
        zone_id=1,
        urgent_bins=(
            BinCandidate(
                bin_id="BIN-001",
                zone_id=1,
                lat=6.927,
                lng=79.861,
                waste_category="food_waste",
                volume_litres=240.0,
                fill_level_pct=85.0,
                urgency_score=90,
                status="critical",
                estimated_weight_kg=160.0,
                battery_level_pct=91.0,
                predicted_full_at=None,
                last_reading_at="2026-04-27T08:00:00Z",
            ),
        ),
        vehicles=(
            VehicleProfile(
                vehicle_id="LORRY-01",
                registration="WP-TRK-1001",
                max_cargo_kg=1000.0,
                volume_m3=12.0,
                waste_categories_supported=("food_waste",),
                active=True,
            ),
        ),
        resolved_at=datetime.now(timezone.utc),
    )


def _plan() -> OptimizationPlan:
    return OptimizationPlan(
        zone_id=1,
        solver_used="ortools",
        routes=(
            VehicleRoutePlan(
                vehicle_id="LORRY-01",
                route_type="emergency",
                stops=(
                    RouteStop(sequence_number=1, bin_id="BIN-001", estimated_arrival_min=14),
                ),
                estimated_weight_kg=160.0,
                estimated_distance_km=1.4,
                estimated_minutes=14,
            ),
        ),
        unassigned_bins=(),
    )


class Stage3DeliveryTests(unittest.TestCase):
    def test_build_deterministic_job_id_is_stable(self):
        snapshot = _snapshot()
        first = build_deterministic_job_id(snapshot)
        second = build_deterministic_job_id(snapshot)
        self.assertEqual(first, second)

    def test_persist_skips_when_job_exists(self):
        repository = _FakeRepository(already_exists=True)
        result = persist_optimization_plan(repository, _snapshot(), _plan(), "job-1")
        self.assertTrue(result.already_exists)
        self.assertEqual(result.inserted_rows, 0)
        self.assertEqual(len(repository.saved_calls), 0)

    def test_persist_writes_routes_when_new_job(self):
        repository = _FakeRepository(already_exists=False)
        result = persist_optimization_plan(repository, _snapshot(), _plan(), "job-2")
        self.assertFalse(result.already_exists)
        self.assertEqual(result.inserted_rows, 1)
        self.assertEqual(len(repository.saved_calls), 1)

    def test_build_optimized_event_contract(self):
        event = build_optimized_route_event(_snapshot(), _plan(), "job-3")
        self.assertEqual(event["version"], "1.0")
        self.assertEqual(event["source_service"], "route-optimizer")
        self.assertEqual(event["payload"]["job_id"], "job-3")
        self.assertEqual(event["payload"]["zone_id"], 1)
        self.assertEqual(event["payload"]["routes"][0]["vehicle_id"], "LORRY-01")
        self.assertEqual(event["payload"]["routes"][0]["bins"], ["BIN-001"])

    def test_publish_uses_send_and_flush(self):
        producer = _FakeProducer()
        event = build_optimized_route_event(_snapshot(), _plan(), "job-4")
        publish_optimized_route_event(producer, "waste.routes.optimized", event)
        self.assertEqual(len(producer.sent), 1)
        self.assertEqual(producer.sent[0][0], "waste.routes.optimized")
        self.assertEqual(producer.flush_calls, 1)


if __name__ == "__main__":
    unittest.main()