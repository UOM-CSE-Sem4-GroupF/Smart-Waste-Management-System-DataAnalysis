from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from config import Settings
from models import EmergencyOptimizationSnapshot, EmergencyTrigger, OptimizationPlan
from repository import RouteOptimizerRepository, normalize_snapshot, normalize_vehicles


@dataclass(frozen=True)
class PreparationResult:
    snapshot: EmergencyOptimizationSnapshot
    urgent_bins_count: int
    vehicle_count: int


@dataclass(frozen=True)
class PersistenceResult:
    job_id: str
    inserted_rows: int
    already_exists: bool


def parse_trigger(event: dict[str, Any], urgency_threshold: int) -> EmergencyTrigger | None:
    payload = event.get("payload") or {}
    urgency_score = int(payload.get("urgency_score", 0))
    if urgency_score < urgency_threshold:
        return None

    trigger_bin_id = payload.get("bin_id")
    if not trigger_bin_id:
        raise ValueError("Event payload is missing bin_id")

    return EmergencyTrigger(
        event_id=str(event.get("timestamp", datetime.now(timezone.utc).isoformat())),
        trigger_bin_id=str(trigger_bin_id),
        zone_id=int(payload["zone_id"]) if payload.get("zone_id") is not None else None,
        urgency_score=urgency_score,
        route_type=str(payload.get("route_type", "emergency")),
        event_timestamp=str(event.get("timestamp", datetime.now(timezone.utc).isoformat())),
        payload=payload,
    )


def prepare_emergency_run(event: dict[str, Any], repository: RouteOptimizerRepository, settings: Settings) -> PreparationResult | None:
    trigger = parse_trigger(event, settings.urgency_threshold)
    if trigger is None:
        return None

    raw_rows = repository.load_emergency_snapshot(trigger.zone_id, trigger.trigger_bin_id, settings.urgency_threshold)
    urgent_bins = normalize_snapshot(event, raw_rows)
    vehicles = normalize_vehicles(raw_rows.vehicles)
    snapshot = EmergencyOptimizationSnapshot(
        trigger=trigger,
        zone_id=raw_rows.zone_id,
        urgent_bins=urgent_bins,
        vehicles=vehicles,
        resolved_at=datetime.now(timezone.utc),
    )
    return PreparationResult(
        snapshot=snapshot,
        urgent_bins_count=len(urgent_bins),
        vehicle_count=len(vehicles),
    )


def build_deterministic_job_id(snapshot: EmergencyOptimizationSnapshot) -> str:
    key = f"{snapshot.trigger.event_id}|{snapshot.trigger.trigger_bin_id}|{snapshot.zone_id}|{snapshot.trigger.route_type}"
    return str(uuid5(NAMESPACE_URL, key))


def persist_optimization_plan(
    repository: RouteOptimizerRepository,
    snapshot: EmergencyOptimizationSnapshot,
    plan: OptimizationPlan,
    job_id: str,
) -> PersistenceResult:
    if repository.route_plan_exists(job_id):
        return PersistenceResult(job_id=job_id, inserted_rows=0, already_exists=True)

    inserted_rows = repository.save_optimization_plan(
        job_id=job_id,
        zone_id=snapshot.zone_id,
        route_type=snapshot.trigger.route_type,
        routes=plan.routes,
    )
    return PersistenceResult(job_id=job_id, inserted_rows=inserted_rows, already_exists=False)


def build_optimized_route_event(
    snapshot: EmergencyOptimizationSnapshot,
    plan: OptimizationPlan,
    job_id: str,
    source_service: str = "route-optimizer",
) -> dict[str, Any]:
    return {
        "version": "1.0",
        "source_service": source_service,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": {
            "job_id": job_id,
            "zone_id": snapshot.zone_id,
            "route_type": snapshot.trigger.route_type,
            "solver_used": plan.solver_used,
            "routes": [
                {
                    "vehicle_id": route.vehicle_id,
                    "bins": [stop.bin_id for stop in route.stops],
                    "estimated_weight_kg": route.estimated_weight_kg,
                    "estimated_distance_km": route.estimated_distance_km,
                    "estimated_minutes": route.estimated_minutes,
                }
                for route in plan.routes
            ],
            "unassigned_bins": list(plan.unassigned_bins),
            "total_estimated_weight_kg": plan.total_weight_kg,
        },
    }


def publish_optimized_route_event(producer: Any, topic: str, event: dict[str, Any]) -> None:
    producer.send(topic, event)
    producer.flush()