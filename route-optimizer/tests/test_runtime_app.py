from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from types import SimpleNamespace
import unittest

from app import handle_event


@dataclass
class _Settings:
    kafka_output_topic: str = "waste.routes.optimized"
    urgency_threshold: int = 70


class _FakeRepository:
    pass


class _FakeProducer:
    pass


class RuntimeAppTests(unittest.TestCase):
    def test_handle_event_commits_on_non_urgent_skip(self):
        from app import prepare_emergency_run as original_prepare
        import app

        def fake_prepare(event, repository, settings):
            return None

        app.prepare_emergency_run = fake_prepare
        try:
            committed = handle_event(
                {"payload": {"bin_id": "BIN-001"}},
                _FakeRepository(),
                _FakeProducer(),
                _Settings(),
                set(),
                deque(),
            )
            self.assertTrue(committed)
        finally:
            app.prepare_emergency_run = original_prepare

    def test_handle_event_commits_on_duplicate_job(self):
        import app
        from models import EmergencyOptimizationSnapshot, EmergencyTrigger

        original_prepare = app.prepare_emergency_run
        original_build_job = app.build_deterministic_job_id

        snapshot = EmergencyOptimizationSnapshot(
            trigger=EmergencyTrigger(
                event_id="e1",
                trigger_bin_id="BIN-001",
                zone_id=1,
                urgency_score=90,
                route_type="emergency",
                event_timestamp="2026-04-27T00:00:00Z",
                payload={},
            ),
            zone_id=1,
        )

        app.prepare_emergency_run = lambda event, repository, settings: SimpleNamespace(
            snapshot=snapshot,
            urgent_bins_count=1,
            vehicle_count=1,
        )
        app.build_deterministic_job_id = lambda s: "job-dup"
        try:
            committed = handle_event(
                {"payload": {"bin_id": "BIN-001"}},
                _FakeRepository(),
                _FakeProducer(),
                _Settings(),
                {"job-dup"},
                deque(["job-dup"]),
            )
            self.assertTrue(committed)
        finally:
            app.prepare_emergency_run = original_prepare
            app.build_deterministic_job_id = original_build_job

    def test_handle_event_persists_and_publishes(self):
        import app
        from models import EmergencyOptimizationSnapshot, EmergencyTrigger, OptimizationPlan

        original_prepare = app.prepare_emergency_run
        original_solve = app.solve_emergency_routes
        original_build_job = app.build_deterministic_job_id
        original_persist = app.persist_optimization_plan
        original_build_event = app.build_optimized_route_event
        original_publish = app.publish_optimized_route_event

        snapshot = EmergencyOptimizationSnapshot(
            trigger=EmergencyTrigger(
                event_id="e2",
                trigger_bin_id="BIN-002",
                zone_id=1,
                urgency_score=90,
                route_type="emergency",
                event_timestamp="2026-04-27T00:00:00Z",
                payload={},
            ),
            zone_id=1,
        )

        persist = SimpleNamespace(already_exists=False, inserted_rows=1)

        captured = {"published": False}

        app.prepare_emergency_run = lambda event, repository, settings: SimpleNamespace(
            snapshot=snapshot,
            urgent_bins_count=2,
            vehicle_count=1,
        )
        app.solve_emergency_routes = lambda snapshot: OptimizationPlan(
            zone_id=1, solver_used="greedy-fallback", routes=(), unassigned_bins=()
        )
        app.build_deterministic_job_id = lambda s: "job-new"
        app.persist_optimization_plan = lambda repository, snapshot, plan, job_id: persist
        app.build_optimized_route_event = lambda snapshot, plan, job_id: {"payload": {"job_id": job_id}}
        app.publish_optimized_route_event = lambda producer, topic, event: captured.__setitem__("published", True)

        processed_ids = set()
        processed_order = deque()
        try:
            committed = handle_event(
                {"payload": {"bin_id": "BIN-002"}},
                _FakeRepository(),
                _FakeProducer(),
                _Settings(),
                processed_ids,
                processed_order,
            )
            self.assertTrue(committed)
            self.assertTrue(captured["published"])
            self.assertIn("job-new", processed_ids)
            self.assertIn("job-new", processed_order)
        finally:
            app.prepare_emergency_run = original_prepare
            app.solve_emergency_routes = original_solve
            app.build_deterministic_job_id = original_build_job
            app.persist_optimization_plan = original_persist
            app.build_optimized_route_event = original_build_event
            app.publish_optimized_route_event = original_publish


if __name__ == "__main__":
    unittest.main()