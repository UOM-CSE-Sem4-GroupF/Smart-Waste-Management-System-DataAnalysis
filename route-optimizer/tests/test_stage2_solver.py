from __future__ import annotations

from datetime import datetime, timezone
import unittest

from models import BinCandidate, EmergencyOptimizationSnapshot, EmergencyTrigger, VehicleProfile
from solver import solve_emergency_routes


def _snapshot(urgent_bins: tuple[BinCandidate, ...], vehicles: tuple[VehicleProfile, ...]) -> EmergencyOptimizationSnapshot:
    return EmergencyOptimizationSnapshot(
        trigger=EmergencyTrigger(
            event_id="event-1",
            trigger_bin_id=urgent_bins[0].bin_id if urgent_bins else "BIN-000",
            zone_id=1,
            urgency_score=85,
            route_type="emergency",
            event_timestamp=datetime.now(timezone.utc).isoformat(),
            payload={},
        ),
        zone_id=1,
        urgent_bins=urgent_bins,
        vehicles=vehicles,
        resolved_at=datetime.now(timezone.utc),
    )


class Stage2SolverTestCase(unittest.TestCase):
    def test_greedy_fallback_respects_capacity_and_category(self):
        bins = (
            BinCandidate(
                bin_id="BIN-001",
                zone_id=1,
                lat=6.9270,
                lng=79.8612,
                waste_category="food_waste",
                volume_litres=240.0,
                fill_level_pct=85.0,
                urgency_score=90,
                status="critical",
                estimated_weight_kg=180.0,
                battery_level_pct=90.0,
                predicted_full_at=None,
                last_reading_at="2026-04-27T08:00:00Z",
            ),
            BinCandidate(
                bin_id="BIN-002",
                zone_id=1,
                lat=6.9280,
                lng=79.8620,
                waste_category="glass",
                volume_litres=120.0,
                fill_level_pct=92.0,
                urgency_score=95,
                status="critical",
                estimated_weight_kg=220.0,
                battery_level_pct=88.0,
                predicted_full_at=None,
                last_reading_at="2026-04-27T08:01:00Z",
            ),
            BinCandidate(
                bin_id="BIN-003",
                zone_id=1,
                lat=6.9295,
                lng=79.8633,
                waste_category="food_waste",
                volume_litres=240.0,
                fill_level_pct=80.0,
                urgency_score=83,
                status="urgent",
                estimated_weight_kg=170.0,
                battery_level_pct=87.0,
                predicted_full_at=None,
                last_reading_at="2026-04-27T08:02:00Z",
            ),
        )

        vehicles = (
            VehicleProfile(
                vehicle_id="LORRY-01",
                registration="WP-TRK-1001",
                max_cargo_kg=300.0,
                volume_m3=10.0,
                waste_categories_supported=("food_waste",),
                active=True,
            ),
            VehicleProfile(
                vehicle_id="LORRY-02",
                registration="WP-TRK-1002",
                max_cargo_kg=250.0,
                volume_m3=8.0,
                waste_categories_supported=("glass",),
                active=True,
            ),
        )

        plan = solve_emergency_routes(_snapshot(bins, vehicles), use_ortools=False)

        self.assertEqual(plan.solver_used, "greedy-fallback")
        route_map = {route.vehicle_id: route for route in plan.routes}
        self.assertIn("LORRY-01", route_map)
        self.assertIn("LORRY-02", route_map)

        assigned_bins = {stop.bin_id for route in plan.routes for stop in route.stops}
        self.assertIn("BIN-001", assigned_bins)
        self.assertIn("BIN-002", assigned_bins)
        self.assertIn("BIN-003", plan.unassigned_bins)

    def test_empty_or_missing_vehicles_returns_unassigned(self):
        bins = (
            BinCandidate(
                bin_id="BIN-001",
                zone_id=1,
                lat=6.9270,
                lng=79.8612,
                waste_category="food_waste",
                volume_litres=240.0,
                fill_level_pct=85.0,
                urgency_score=90,
                status="critical",
                estimated_weight_kg=180.0,
                battery_level_pct=90.0,
                predicted_full_at=None,
                last_reading_at="2026-04-27T08:00:00Z",
            ),
        )

        plan = solve_emergency_routes(_snapshot(bins, ()), use_ortools=False)
        self.assertEqual(plan.solver_used, "none")
        self.assertEqual(plan.unassigned_bins, ("BIN-001",))
        self.assertEqual(len(plan.routes), 0)

    @unittest.skipUnless(__import__("importlib").util.find_spec("ortools") is not None, "ortools not installed")
    def test_ortools_solver_path_returns_plan(self):
        bins = (
            BinCandidate(
                bin_id="BIN-001",
                zone_id=1,
                lat=6.9270,
                lng=79.8612,
                waste_category="food_waste",
                volume_litres=240.0,
                fill_level_pct=85.0,
                urgency_score=90,
                status="critical",
                estimated_weight_kg=160.0,
                battery_level_pct=90.0,
                predicted_full_at=None,
                last_reading_at="2026-04-27T08:00:00Z",
            ),
            BinCandidate(
                bin_id="BIN-002",
                zone_id=1,
                lat=6.9280,
                lng=79.8620,
                waste_category="food_waste",
                volume_litres=240.0,
                fill_level_pct=80.0,
                urgency_score=82,
                status="urgent",
                estimated_weight_kg=140.0,
                battery_level_pct=88.0,
                predicted_full_at=None,
                last_reading_at="2026-04-27T08:01:00Z",
            ),
        )
        vehicles = (
            VehicleProfile(
                vehicle_id="LORRY-01",
                registration="WP-TRK-1001",
                max_cargo_kg=1000.0,
                volume_m3=12.0,
                waste_categories_supported=("food_waste",),
                active=True,
            ),
        )

        plan = solve_emergency_routes(_snapshot(bins, vehicles), use_ortools=True)

        self.assertIn(plan.solver_used, ("ortools", "greedy-fallback"))
        assigned = {stop.bin_id for route in plan.routes for stop in route.stops}
        self.assertIn("BIN-001", assigned)
        self.assertIn("BIN-002", assigned)


if __name__ == "__main__":
    unittest.main()