"""
Tests for POST /internal/route-optimizer/solve and GET /health.
Covers all acceptance criteria from spec §9.
"""
from __future__ import annotations

import sys
import os
import unittest

# Allow imports from the parent directory (route-optimizer package root)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from fastapi.testclient import TestClient

from app import app
from models import (
    BinInput,
    ClusterInput,
    DepotInput,
    SolveRequest,
    VehicleInput,
)

client = TestClient(app)


# ── Helpers ───────────────────────────────────────────────────────────────────

DEPOT = DepotInput(lat=6.9200, lng=79.8600)

def _vehicle(vid: str, capacity_kg: float, categories: list[str],
             lat: float = 6.9200, lng: float = 79.8600) -> VehicleInput:
    return VehicleInput(
        vehicle_id=vid,
        max_cargo_kg=capacity_kg,
        waste_categories_supported=categories,
        current_lat=lat,
        current_lng=lng,
    )

def _cluster(cid: str, lat: float, lng: float) -> ClusterInput:
    return ClusterInput(cluster_id=cid, lat=lat, lng=lng, cluster_name=f"Zone {cid}")

def _bin(bid: str, cid: str, lat: float, lng: float,
         category: str, weight_kg: float, urgency: int) -> BinInput:
    return BinInput(
        bin_id=bid,
        cluster_id=cid,
        lat=lat,
        lng=lng,
        waste_category=category,
        fill_level_pct=80.0,
        estimated_weight_kg=weight_kg,
        urgency_score=urgency,
    )

def _basic_request(**overrides) -> dict:
    req = SolveRequest(
        job_id="job-test-001",
        job_type="routine",
        clusters=[
            _cluster("C1", 6.9270, 79.8612),
            _cluster("C2", 6.9300, 79.8650),
        ],
        bins=[
            _bin("BIN-001", "C1", 6.9270, 79.8612, "food_waste", 80.0, 85),
            _bin("BIN-002", "C2", 6.9300, 79.8650, "food_waste", 60.0, 75),
        ],
        available_vehicles=[
            _vehicle("LORRY-01", 500.0, ["food_waste"]),
        ],
        depot=DEPOT,
        time_limit_seconds=5,
    )
    return req.model_dump() | overrides


# ── Health endpoint ───────────────────────────────────────────────────────────

class HealthTest(unittest.TestCase):
    def test_health_returns_ok(self):
        r = client.get("/health")
        self.assertEqual(r.status_code, 200)
        body = r.json()
        self.assertEqual(body["status"], "ok")
        self.assertEqual(body["service"], "route-optimizer")


# ── Happy-path solve ──────────────────────────────────────────────────────────

class SolveHappyPathTest(unittest.TestCase):
    def setUp(self):
        r = client.post("/internal/route-optimizer/solve", json=_basic_request())
        self.assertEqual(r.status_code, 200, r.text)
        self.body = r.json()

    def test_success_flag_true(self):
        self.assertTrue(self.body["success"])

    def test_job_id_echoed(self):
        self.assertEqual(self.body["job_id"], "job-test-001")

    def test_method_is_known(self):
        self.assertIn(self.body["method"], ("or_tools", "nearest_neighbour_fallback"))

    def test_waypoints_returned(self):
        self.assertGreater(len(self.body["waypoints"]), 0)

    def test_waypoints_in_sequence_order(self):
        seqs = [w["sequence"] for w in self.body["waypoints"]]
        self.assertEqual(seqs, list(range(1, len(seqs) + 1)))

    def test_estimated_arrival_iso_valid(self):
        from datetime import datetime
        for wp in self.body["waypoints"]:
            # Should parse without error
            iso = wp["estimated_arrival_iso"].rstrip("Z")
            datetime.fromisoformat(iso)

    def test_cumulative_weight_never_exceeds_vehicle_capacity(self):
        capacity = 500.0
        for wp in self.body["waypoints"]:
            self.assertLessEqual(wp["cumulative_weight_kg"], capacity)

    def test_total_weight_matches_last_waypoint(self):
        last_wp = self.body["waypoints"][-1]
        self.assertAlmostEqual(
            self.body["total_weight_kg"], last_wp["cumulative_weight_kg"], places=2
        )

    def test_solver_time_ms_positive(self):
        self.assertGreater(self.body["solver_time_ms"], 0)

    def test_total_distance_km_positive(self):
        self.assertGreater(self.body["total_distance_km"], 0)


# ── Waste category constraint ─────────────────────────────────────────────────

class WasteCategoryConstraintTest(unittest.TestCase):
    def test_bins_from_incompatible_category_trigger_400(self):
        req = _basic_request()
        # Add a glass bin but only food_waste vehicle available
        req["bins"].append({
            "bin_id": "BIN-GLASS",
            "cluster_id": "C1",
            "lat": 6.9270,
            "lng": 79.8612,
            "waste_category": "glass",
            "fill_level_pct": 90.0,
            "estimated_weight_kg": 50.0,
            "urgency_score": 80,
        })
        r = client.post("/internal/route-optimizer/solve", json=req)
        self.assertEqual(r.status_code, 400)
        self.assertEqual(r.json()["error"], "INVALID_REQUEST")

    def test_multi_vehicle_category_split(self):
        """Two vehicles each serving different waste categories."""
        req = SolveRequest(
            job_id="job-split-cat",
            job_type="routine",
            clusters=[
                _cluster("C1", 6.9270, 79.8612),
                _cluster("C2", 6.9280, 79.8625),
            ],
            bins=[
                _bin("BIN-F", "C1", 6.9270, 79.8612, "food_waste", 80.0, 80),
                _bin("BIN-G", "C2", 6.9280, 79.8625, "glass", 50.0, 80),
            ],
            available_vehicles=[
                _vehicle("VEH-FOOD", 500.0, ["food_waste"]),
                _vehicle("VEH-GLASS", 500.0, ["glass"]),
            ],
            depot=DEPOT,
            time_limit_seconds=5,
        )
        r = client.post("/internal/route-optimizer/solve", json=req.model_dump())
        self.assertEqual(r.status_code, 200, r.text)
        body = r.json()
        self.assertTrue(body["success"])


# ── Capacity constraint ───────────────────────────────────────────────────────

class CapacityConstraintTest(unittest.TestCase):
    def test_total_weight_exceeding_all_vehicles_returns_422(self):
        req = _basic_request()
        # Make bins very heavy
        for b in req["bins"]:
            b["estimated_weight_kg"] = 10_000.0
        r = client.post("/internal/route-optimizer/solve", json=req)
        self.assertEqual(r.status_code, 422)
        self.assertEqual(r.json()["error"], "NO_FEASIBLE_SOLUTION")


# ── Urgency / time windows ────────────────────────────────────────────────────

class UrgencyTimeWindowTest(unittest.TestCase):
    def test_urgency_to_deadline_mapping(self):
        from solver import urgency_to_deadline_minutes
        self.assertEqual(urgency_to_deadline_minutes(95), 60)
        self.assertEqual(urgency_to_deadline_minutes(85), 120)
        self.assertEqual(urgency_to_deadline_minutes(72), 240)
        self.assertEqual(urgency_to_deadline_minutes(50), 480)

    def test_high_urgency_cluster_has_tight_deadline_in_waypoint(self):
        """Cluster with urgency ≥ 90 should have a deadline ≤ 60 min from now."""
        from datetime import datetime, timezone, timedelta
        req = SolveRequest(
            job_id="job-urgency",
            job_type="emergency",
            clusters=[_cluster("CU", 6.9270, 79.8612)],
            bins=[_bin("BIN-U", "CU", 6.9270, 79.8612, "food_waste", 50.0, 95)],
            available_vehicles=[_vehicle("V1", 500.0, ["food_waste"])],
            depot=DEPOT,
            time_limit_seconds=5,
        )
        r = client.post("/internal/route-optimizer/solve", json=req.model_dump())
        self.assertEqual(r.status_code, 200, r.text)
        wp = r.json()["waypoints"][0]
        deadline = datetime.fromisoformat(wp["time_window_deadline_iso"].rstrip("Z"))
        now = datetime.utcnow()
        diff_minutes = (deadline - now).total_seconds() / 60
        self.assertLessEqual(diff_minutes, 65)   # 60 min + small tolerance


# ── Haversine / distance matrix ───────────────────────────────────────────────

class HaversineTest(unittest.TestCase):
    def test_haversine_known_distance(self):
        """Colombo Fort to Galle Fort is ≈ 116 km."""
        from solver import haversine_metres
        dist_m = haversine_metres(6.9344, 79.8428, 6.0263, 80.2170)
        self.assertAlmostEqual(dist_m / 1000, 116, delta=5)

    def test_distance_matrix_symmetric(self):
        from solver import build_distance_matrix
        locs = [
            {"lat": 6.9270, "lng": 79.8612},
            {"lat": 6.9300, "lng": 79.8650},
            {"lat": 6.9200, "lng": 79.8600},
        ]
        mat = build_distance_matrix(locs)
        for i in range(3):
            self.assertEqual(mat[i][i], 0)
            for j in range(3):
                self.assertEqual(mat[i][j], mat[j][i])

    def test_distance_matrix_uses_integer_metres(self):
        from solver import build_distance_matrix
        locs = [{"lat": 6.9270, "lng": 79.8612}, {"lat": 6.9300, "lng": 79.8650}]
        mat = build_distance_matrix(locs)
        self.assertIsInstance(mat[0][1], int)
        self.assertIsInstance(mat[1][0], int)


# ── Data model builder ────────────────────────────────────────────────────────

class DataModelBuilderTest(unittest.TestCase):
    def _make_request(self) -> "SolveRequest":
        return SolveRequest(
            job_id="j1",
            job_type="routine",
            clusters=[
                _cluster("C1", 6.9270, 79.8612),
                _cluster("C2", 6.9300, 79.8650),
            ],
            bins=[
                _bin("B1", "C1", 6.9270, 79.8612, "food_waste", 100.0, 85),
                _bin("B2", "C1", 6.9270, 79.8612, "food_waste", 50.0, 70),
                _bin("B3", "C2", 6.9300, 79.8650, "food_waste", 80.0, 75),
            ],
            available_vehicles=[
                _vehicle("V1", 500.0, ["food_waste"]),
                _vehicle("V2", 300.0, ["food_waste"]),
            ],
            depot=DEPOT,
            time_limit_seconds=5,
        )

    def test_location_ordering(self):
        from solver import build_data_model
        req = self._make_request()
        data = build_data_model(req)
        n_v = data["n_vehicles"]  # 2
        n_c = data["n_clusters"]  # 2
        # Total = 2 vehicles + 2 clusters + 1 depot = 5
        self.assertEqual(len(data["locations"]), n_v + n_c + 1)
        self.assertEqual(data["depot_index"], n_v + n_c)

    def test_demands_use_integer_arithmetic(self):
        from solver import build_data_model
        req = self._make_request()
        data = build_data_model(req)
        n_v = data["n_vehicles"]
        # C1 demand: (100.0 + 50.0) * 100 = 15000
        self.assertEqual(data["demands"][n_v], 15000)
        # C2 demand: 80.0 * 100 = 8000
        self.assertEqual(data["demands"][n_v + 1], 8000)

    def test_service_time_per_cluster(self):
        from solver import build_data_model
        req = self._make_request()
        data = build_data_model(req)
        n_v = data["n_vehicles"]
        # C1 has 2 bins → 10 min, C2 has 1 bin → 5 min
        self.assertEqual(data["service_times"][n_v], 10)
        self.assertEqual(data["service_times"][n_v + 1], 5)

    def test_starts_and_ends(self):
        from solver import build_data_model
        req = self._make_request()
        data = build_data_model(req)
        self.assertEqual(data["starts"], [0, 1])
        self.assertEqual(data["ends"], [data["depot_index"], data["depot_index"]])


# ── Fallback solver ───────────────────────────────────────────────────────────

class NearestNeighbourFallbackTest(unittest.TestCase):
    def test_fallback_returns_valid_response(self):
        import time
        from solver import _nearest_neighbour_fallback
        req = SolveRequest(
            job_id="j-fallback",
            job_type="routine",
            clusters=[
                _cluster("C1", 6.9270, 79.8612),
                _cluster("C2", 6.9300, 79.8650),
            ],
            bins=[
                _bin("B1", "C1", 6.9270, 79.8612, "food_waste", 80.0, 80),
                _bin("B2", "C2", 6.9300, 79.8650, "food_waste", 60.0, 70),
            ],
            available_vehicles=[_vehicle("V1", 500.0, ["food_waste"])],
            depot=DEPOT,
            time_limit_seconds=5,
        )
        result = _nearest_neighbour_fallback(req, time.time())
        self.assertEqual(result.method, "nearest_neighbour_fallback")
        self.assertEqual(result.job_id, "j-fallback")
        self.assertGreater(len(result.waypoints), 0)
        self.assertTrue(result.success)

    def test_fallback_waypoints_in_sequence(self):
        import time
        from solver import _nearest_neighbour_fallback
        req = SolveRequest(
            job_id="j-seq",
            job_type="routine",
            clusters=[_cluster(f"C{i}", 6.92 + i * 0.005, 79.86) for i in range(4)],
            bins=[_bin(f"B{i}", f"C{i}", 6.92 + i * 0.005, 79.86, "food_waste", 40.0, 75) for i in range(4)],
            available_vehicles=[_vehicle("V1", 1000.0, ["food_waste"])],
            depot=DEPOT,
            time_limit_seconds=5,
        )
        result = _nearest_neighbour_fallback(req, time.time())
        seqs = [w.sequence for w in result.waypoints]
        self.assertEqual(seqs, list(range(1, len(seqs) + 1)))


# ── Validate request ──────────────────────────────────────────────────────────

class ValidateRequestTest(unittest.TestCase):
    def test_no_clusters_raises_invalid(self):
        from solver import validate_request, InvalidRequestError
        req = SolveRequest(
            job_id="j", job_type="routine",
            clusters=[], bins=[],
            available_vehicles=[_vehicle("V1", 100.0, ["food_waste"])],
            depot=DEPOT,
        )
        with self.assertRaises(InvalidRequestError):
            validate_request(req)

    def test_no_vehicles_raises_invalid(self):
        from solver import validate_request, InvalidRequestError
        req = SolveRequest(
            job_id="j", job_type="routine",
            clusters=[_cluster("C1", 6.9, 79.8)],
            bins=[_bin("B1", "C1", 6.9, 79.8, "food_waste", 50.0, 80)],
            available_vehicles=[],
            depot=DEPOT,
        )
        with self.assertRaises(InvalidRequestError):
            validate_request(req)

    def test_unsupported_category_raises_invalid(self):
        from solver import validate_request, InvalidRequestError
        req = SolveRequest(
            job_id="j", job_type="routine",
            clusters=[_cluster("C1", 6.9, 79.8)],
            bins=[_bin("B1", "C1", 6.9, 79.8, "hazardous", 50.0, 80)],
            available_vehicles=[_vehicle("V1", 500.0, ["food_waste"])],
            depot=DEPOT,
        )
        with self.assertRaises(InvalidRequestError):
            validate_request(req)

    def test_overweight_raises_no_feasible(self):
        from solver import validate_request, NoFeasibleSolutionError
        req = SolveRequest(
            job_id="j", job_type="routine",
            clusters=[_cluster("C1", 6.9, 79.8)],
            bins=[_bin("B1", "C1", 6.9, 79.8, "food_waste", 99999.0, 80)],
            available_vehicles=[_vehicle("V1", 500.0, ["food_waste"])],
            depot=DEPOT,
        )
        with self.assertRaises(NoFeasibleSolutionError):
            validate_request(req)


if __name__ == "__main__":
    unittest.main()
