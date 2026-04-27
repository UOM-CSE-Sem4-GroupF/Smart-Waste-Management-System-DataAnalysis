from __future__ import annotations

import unittest

from config import load_settings
from repository import EmergencySnapshotRows, RouteOptimizerRepository, normalize_snapshot, normalize_vehicles
from service import prepare_emergency_run


class FakeCursor:
    def __init__(self, responses):
        self.responses = responses
        self.executed = []
        self.index = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params=None):
        self.executed.append((query.strip(), params))

    def fetchone(self):
        response = self.responses[self.index]
        self.index += 1
        return response

    def fetchall(self):
        response = self.responses[self.index]
        self.index += 1
        return response


class FakeConnection:
    def __init__(self, responses):
        self.cursor_obj = FakeCursor(responses)
        self.closed = False

    def cursor(self, *args, **kwargs):
        return self.cursor_obj

    def close(self):
        self.closed = True


class RepositoryTestCase(unittest.TestCase):
    def test_resolve_zone_and_load_snapshot_queries(self):
        urgent_row = {
            "bin_id": "BIN-001",
            "zone_id": 1,
            "lat": 6.927079,
            "lng": 79.861244,
            "volume_litres": 240.0,
            "waste_category": "food_waste",
            "avg_kg_per_litre": 0.9,
            "fill_level_pct": 82.4,
            "status": "urgent",
            "urgency_score": 81,
            "estimated_weight_kg": 177.98,
            "battery_level_pct": 91.0,
            "predicted_full_at": None,
            "last_reading_at": "2026-04-24T08:00:00Z",
        }
        vehicle_row = {
            "id": "LORRY-01",
            "registration": "WP-TRK-1001",
            "max_cargo_kg": 1500.0,
            "volume_m3": 12.0,
            "waste_categories_supported": ["food_waste", "general"],
            "active": True,
        }
        connection = FakeConnection([
            (1,),
            [urgent_row],
            [vehicle_row],
        ])
        repository = RouteOptimizerRepository(lambda: connection)

        rows = repository.load_emergency_snapshot(None, "BIN-001", 70)

        self.assertEqual(rows.zone_id, 1)
        self.assertEqual(len(rows.urgent_bins), 1)
        self.assertEqual(len(rows.vehicles), 1)
        self.assertIn("SELECT zone_id FROM bins", connection.cursor_obj.executed[0][0])
        self.assertIn("c.urgency_score >= %s", connection.cursor_obj.executed[1][0])
        self.assertIn("FROM vehicles", connection.cursor_obj.executed[2][0])

    def test_normalize_snapshot_uses_db_weight_when_present(self):
        rows = EmergencySnapshotRows(
            zone_id=1,
            urgent_bins=(
                {
                    "bin_id": "BIN-001",
                    "zone_id": 1,
                    "lat": 6.927079,
                    "lng": 79.861244,
                    "volume_litres": 240.0,
                    "waste_category": "food_waste",
                    "avg_kg_per_litre": 0.9,
                    "fill_level_pct": 82.4,
                    "status": "urgent",
                    "urgency_score": 81,
                    "estimated_weight_kg": 177.98,
                    "battery_level_pct": 91.0,
                    "predicted_full_at": None,
                    "last_reading_at": "2026-04-24T08:00:00Z",
                },
            ),
            vehicles=(),
        )
        normalized = normalize_snapshot({}, rows)
        self.assertEqual(len(normalized), 1)
        self.assertEqual(normalized[0].estimated_weight_kg, 177.98)

    def test_normalize_vehicles_flattens_supported_categories(self):
        vehicles = normalize_vehicles([
            {
                "id": "LORRY-01",
                "registration": "WP-TRK-1001",
                "max_cargo_kg": 1500.0,
                "volume_m3": 12.0,
                "waste_categories_supported": ["food_waste", "general"],
                "active": True,
            }
        ])
        self.assertEqual(vehicles[0].waste_categories_supported, ("food_waste", "general"))


class ServiceTestCase(unittest.TestCase):
    def test_prepare_emergency_run_skips_low_urgency_events(self):
        repository = RouteOptimizerRepository(lambda: FakeConnection([]))
        settings = load_settings({"ROUTE_OPTIMIZER_URGENCY_THRESHOLD": "70"})
        result = prepare_emergency_run(
            {
                "timestamp": "2026-04-24T08:00:00Z",
                "payload": {
                    "bin_id": "BIN-999",
                    "urgency_score": 69,
                },
            },
            repository,
            settings,
        )
        self.assertIsNone(result)

    def test_prepare_emergency_run_builds_snapshot(self):
        urgent_row = {
            "bin_id": "BIN-001",
            "zone_id": 1,
            "lat": 6.927079,
            "lng": 79.861244,
            "volume_litres": 240.0,
            "waste_category": "food_waste",
            "avg_kg_per_litre": 0.9,
            "fill_level_pct": 82.4,
            "status": "urgent",
            "urgency_score": 81,
            "estimated_weight_kg": 177.98,
            "battery_level_pct": 91.0,
            "predicted_full_at": None,
            "last_reading_at": "2026-04-24T08:00:00Z",
        }
        vehicle_row = {
            "id": "LORRY-01",
            "registration": "WP-TRK-1001",
            "max_cargo_kg": 1500.0,
            "volume_m3": 12.0,
            "waste_categories_supported": ["food_waste", "general"],
            "active": True,
        }
        connection = FakeConnection([
            (1,),
            [urgent_row],
            [vehicle_row],
        ])
        repository = RouteOptimizerRepository(lambda: connection)
        settings = load_settings({"ROUTE_OPTIMIZER_URGENCY_THRESHOLD": "70"})

        result = prepare_emergency_run(
            {
                "timestamp": "2026-04-24T08:00:00Z",
                "payload": {
                    "bin_id": "BIN-001",
                    "urgency_score": 81,
                },
            },
            repository,
            settings,
        )

        self.assertIsNotNone(result)
        self.assertEqual(result.snapshot.zone_id, 1)
        self.assertEqual(result.urgent_bins_count, 1)
        self.assertEqual(result.vehicle_count, 1)
        self.assertAlmostEqual(result.snapshot.total_estimated_weight_kg, 177.98, places=2)


if __name__ == "__main__":
    unittest.main()