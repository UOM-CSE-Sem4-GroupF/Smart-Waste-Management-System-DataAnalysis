from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class EmergencyTrigger:
    event_id: str
    trigger_bin_id: str
    zone_id: int | None
    urgency_score: int
    route_type: str
    event_timestamp: str
    payload: dict[str, Any]


@dataclass(frozen=True)
class BinCandidate:
    bin_id: str
    zone_id: int
    lat: float
    lng: float
    waste_category: str
    volume_litres: float
    fill_level_pct: float
    urgency_score: int
    status: str
    estimated_weight_kg: float
    battery_level_pct: float | None
    predicted_full_at: str | None
    last_reading_at: str | None


@dataclass(frozen=True)
class VehicleProfile:
    vehicle_id: str
    registration: str
    max_cargo_kg: float
    volume_m3: float | None
    waste_categories_supported: tuple[str, ...]
    active: bool


@dataclass(frozen=True)
class EmergencyOptimizationSnapshot:
    trigger: EmergencyTrigger
    zone_id: int
    urgent_bins: tuple[BinCandidate, ...] = field(default_factory=tuple)
    vehicles: tuple[VehicleProfile, ...] = field(default_factory=tuple)
    resolved_at: datetime | None = None

    @property
    def total_estimated_weight_kg(self) -> float:
        return round(sum(bin_candidate.estimated_weight_kg for bin_candidate in self.urgent_bins), 2)


@dataclass(frozen=True)
class RouteStop:
    sequence_number: int
    bin_id: str
    estimated_arrival_min: int


@dataclass(frozen=True)
class VehicleRoutePlan:
    vehicle_id: str
    route_type: str
    stops: tuple[RouteStop, ...]
    estimated_weight_kg: float
    estimated_distance_km: float
    estimated_minutes: int


@dataclass(frozen=True)
class OptimizationPlan:
    zone_id: int
    solver_used: str
    routes: tuple[VehicleRoutePlan, ...]
    unassigned_bins: tuple[str, ...]

    @property
    def total_weight_kg(self) -> float:
        return round(sum(route.estimated_weight_kg for route in self.routes), 2)