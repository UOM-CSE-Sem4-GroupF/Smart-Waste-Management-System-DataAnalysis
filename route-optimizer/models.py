from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel


class ClusterInput(BaseModel):
    cluster_id: str
    lat: float
    lng: float
    cluster_name: str


class BinInput(BaseModel):
    bin_id: str
    cluster_id: str
    lat: float
    lng: float
    waste_category: str
    fill_level_pct: float
    estimated_weight_kg: float
    urgency_score: int
    predicted_full_at: Optional[str] = None  # ISO 8601 or null


class VehicleInput(BaseModel):
    vehicle_id: str
    max_cargo_kg: float
    waste_categories_supported: List[str]
    current_lat: float
    current_lng: float
    # vehicles start from their current position, not depot
    # for pre-shift routine jobs they start from depot


class DepotInput(BaseModel):
    lat: float
    lng: float
    # all routes end at depot


class SolveRequest(BaseModel):
    job_id: str
    job_type: str                    # routine | emergency

    clusters: List[ClusterInput]
    bins: List[BinInput]
    available_vehicles: List[VehicleInput]

    depot: DepotInput
    time_limit_seconds: int = 30     # solver time limit


class Waypoint(BaseModel):
    sequence: int
    cluster_id: str
    cluster_name: str
    lat: float
    lng: float
    bins: List[str]                  # bin_ids to collect at this stop
    estimated_arrival_iso: str       # ISO 8601 estimated arrival
    time_window_deadline_iso: str    # must arrive before this
    cumulative_weight_kg: float      # running weight total at this stop
    stop_duration_minutes: int       # estimated time at stop


class SolveResponse(BaseModel):
    success: bool
    job_id: str
    method: str                      # 'or_tools' | 'nearest_neighbour_fallback'
    solver_time_ms: int

    vehicle_id: str
    waypoints: List[Waypoint]
    total_distance_km: float
    estimated_minutes: int
    total_weight_kg: float

    # null if fallback was used
    optimality_gap_pct: Optional[float] = None