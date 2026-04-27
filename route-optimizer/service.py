from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from config import Settings
from models import EmergencyOptimizationSnapshot, EmergencyTrigger
from repository import RouteOptimizerRepository, normalize_snapshot, normalize_vehicles


@dataclass(frozen=True)
class PreparationResult:
    snapshot: EmergencyOptimizationSnapshot
    urgent_bins_count: int
    vehicle_count: int


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