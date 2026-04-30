from __future__ import annotations

import math
import time
from datetime import datetime, timedelta
from typing import List

from models import (
    BinInput,
    ClusterInput,
    SolveRequest,
    SolveResponse,
    VehicleInput,
    Waypoint,
)


# ── Custom exceptions ─────────────────────────────────────────────────────────

class InvalidRequestError(Exception):
    pass


class NoFeasibleSolutionError(Exception):
    pass


class SolverTimeoutError(Exception):
    pass


# ── Constants ─────────────────────────────────────────────────────────────────

AVG_SPEED_KMH = 30.0          # km/h average urban speed (spec §6)
SERVICE_MINUTES_PER_BIN = 5   # minutes service time per bin at a stop


# ── Geometry helpers ──────────────────────────────────────────────────────────

def haversine_metres(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Haversine great-circle distance in metres."""
    R = 6_371_000
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlng / 2) ** 2
    )
    return R * 2 * math.asin(math.sqrt(a))


def build_distance_matrix(locations: list) -> list:
    """Integer distance matrix in metres using Haversine formula."""
    n = len(locations)
    matrix = [[0] * n for _ in range(n)]
    for i in range(n):
        for j in range(n):
            if i != j:
                matrix[i][j] = int(
                    haversine_metres(
                        locations[i]["lat"], locations[i]["lng"],
                        locations[j]["lat"], locations[j]["lng"],
                    )
                )
    return matrix


def urgency_to_deadline_minutes(urgency_score: int) -> int:
    """Convert urgency score to collection deadline in minutes from now."""
    if urgency_score >= 90:
        return 60
    if urgency_score >= 80:
        return 120
    if urgency_score >= 70:
        return 240
    return 480  # routine bins — full shift window


# ── Data model builder (spec §6) ──────────────────────────────────────────────

def build_data_model(request: SolveRequest) -> dict:
    """
    Build the VRP data model from the solve request.

    Location ordering:
      [0 .. N-1]       = vehicle start positions  (N = n_vehicles)
      [N .. N+M-1]     = cluster locations         (M = n_clusters)
      [N+M]            = depot (end for all vehicles)
    """
    n_vehicles = len(request.available_vehicles)

    vehicle_starts = [
        {"lat": v.current_lat, "lng": v.current_lng}
        for v in request.available_vehicles
    ]
    cluster_locs = [{"lat": c.lat, "lng": c.lng} for c in request.clusters]
    depot = {"lat": request.depot.lat, "lng": request.depot.lng}

    all_locations = vehicle_starts + cluster_locs + [depot]
    n_total = len(all_locations)
    depot_index = n_total - 1

    data: dict = {}
    data["locations"] = all_locations
    data["n_vehicles"] = n_vehicles
    data["n_clusters"] = len(request.clusters)
    data["depot_index"] = depot_index
    data["starts"] = list(range(n_vehicles))
    data["ends"] = [depot_index] * n_vehicles

    # Distance matrix (metres, integer)
    data["distance_matrix"] = build_distance_matrix(all_locations)

    # Time matrix (minutes, integer) — 30 km/h average
    data["time_matrix"] = [
        [int(data["distance_matrix"][i][j] / 1000 / AVG_SPEED_KMH * 60)
         for j in range(n_total)]
        for i in range(n_total)
    ]

    # Service times (minutes per node)
    service_times = [0] * n_vehicles  # vehicle start nodes
    for cluster in request.clusters:
        bins_here = [b for b in request.bins if b.cluster_id == cluster.cluster_id]
        service_times.append(len(bins_here) * SERVICE_MINUTES_PER_BIN)
    service_times.append(0)  # depot
    data["service_times"] = service_times

    # Demands (weight × 100 for integer arithmetic)
    demands = [0] * n_vehicles  # vehicle start nodes
    for cluster in request.clusters:
        w = sum(
            b.estimated_weight_kg
            for b in request.bins
            if b.cluster_id == cluster.cluster_id
        )
        demands.append(int(w * 100))
    demands.append(0)  # depot
    data["demands"] = demands

    # Vehicle capacities (kg × 100)
    data["vehicle_capacities"] = [int(v.max_cargo_kg * 100) for v in request.available_vehicles]

    # Time windows (minutes from now)
    time_windows = [[0, 480]] * n_vehicles  # vehicle starts: full shift
    for cluster in request.clusters:
        cluster_bins = [b for b in request.bins if b.cluster_id == cluster.cluster_id]
        deadlines = [urgency_to_deadline_minutes(b.urgency_score) for b in cluster_bins]
        tightest = min(deadlines) if deadlines else 480
        time_windows.append([0, tightest])
    time_windows.append([0, 480])  # depot
    data["time_windows"] = time_windows

    data["vehicles"] = request.available_vehicles
    data["clusters"] = request.clusters
    data["bins"] = request.bins
    return data


# ── Request validation ────────────────────────────────────────────────────────

def validate_request(request: SolveRequest) -> None:
    """Raise InvalidRequestError or NoFeasibleSolutionError on bad input."""
    if not request.clusters:
        raise InvalidRequestError("No clusters provided in request")
    if not request.available_vehicles:
        raise InvalidRequestError("No vehicles provided in request")

    # Every waste category must have at least one supporting vehicle
    for category in {b.waste_category for b in request.bins}:
        if not any(
            category in v.waste_categories_supported
            for v in request.available_vehicles
        ):
            raise InvalidRequestError(f"No vehicles support waste category: {category}")

    # Total weight must not exceed combined vehicle capacity
    total_weight = sum(b.estimated_weight_kg for b in request.bins)
    max_combined = sum(v.max_cargo_kg for v in request.available_vehicles)
    if total_weight > max_combined:
        raise NoFeasibleSolutionError(
            f"Total weight {total_weight:,.0f} kg exceeds all available vehicles "
            f"({max_combined:,.0f} kg combined capacity)"
        )


# ── Solution extractor (spec §7) ──────────────────────────────────────────────

def extract_solution(
    manager,
    routing,
    solution,
    request: SolveRequest,
    data: dict,
    solver_time_ms: int,
) -> SolveResponse:
    """Extract the best single-vehicle route from the OR-Tools solution."""
    now = datetime.utcnow()
    time_dimension = routing.GetDimensionOrDie("Time")
    n_vehicles = data["n_vehicles"]
    cluster_offset = n_vehicles  # cluster nodes start here

    best_vehicle_idx = None
    best_route: list = []
    best_distance = float("inf")

    for vehicle_idx in range(n_vehicles):
        index = routing.Start(vehicle_idx)
        route: list = []
        route_distance = 0
        cumulative_weight = 0.0

        while not routing.IsEnd(index):
            node = manager.IndexToNode(index)

            # Process cluster nodes only (skip vehicle starts and depot)
            if cluster_offset <= node < data["depot_index"]:
                cluster_idx = node - cluster_offset
                cluster = request.clusters[cluster_idx]
                cluster_bins = [b for b in request.bins if b.cluster_id == cluster.cluster_id]
                cluster_bin_ids = [b.bin_id for b in cluster_bins]
                cluster_weight = sum(b.estimated_weight_kg for b in cluster_bins)
                cumulative_weight += cluster_weight

                arrival_min = solution.Min(time_dimension.CumulVar(index))
                arrival_time = now + timedelta(minutes=arrival_min)
                deadline_min = data["time_windows"][node][1]
                deadline_time = now + timedelta(minutes=deadline_min)
                stop_duration = len(cluster_bin_ids) * SERVICE_MINUTES_PER_BIN

                route.append(
                    Waypoint(
                        sequence=len(route) + 1,
                        cluster_id=cluster.cluster_id,
                        cluster_name=cluster.cluster_name,
                        lat=cluster.lat,
                        lng=cluster.lng,
                        bins=cluster_bin_ids,
                        estimated_arrival_iso=arrival_time.isoformat() + "Z",
                        time_window_deadline_iso=deadline_time.isoformat() + "Z",
                        cumulative_weight_kg=round(cumulative_weight, 2),
                        stop_duration_minutes=stop_duration,
                    )
                )

            prev_index = index
            index = solution.Value(routing.NextVar(index))
            route_distance += routing.GetArcCostForVehicle(prev_index, index, vehicle_idx)

        if route and route_distance < best_distance:
            best_vehicle_idx = vehicle_idx
            best_route = route
            best_distance = route_distance

    if best_vehicle_idx is None or not best_route:
        raise NoFeasibleSolutionError(
            "OR-Tools produced a solution but no viable vehicle route was extracted"
        )

    vehicle = request.available_vehicles[best_vehicle_idx]
    total_minutes = sum(w.stop_duration_minutes for w in best_route)
    total_minutes += int(best_distance / 1000 / AVG_SPEED_KMH * 60)

    return SolveResponse(
        success=True,
        job_id=request.job_id,
        method="or_tools",
        solver_time_ms=solver_time_ms,
        vehicle_id=vehicle.vehicle_id,
        waypoints=best_route,
        total_distance_km=round(best_distance / 1000, 2),
        estimated_minutes=total_minutes,
        total_weight_kg=best_route[-1].cumulative_weight_kg if best_route else 0.0,
        optimality_gap_pct=None,
    )


# ── Nearest-neighbour fallback ────────────────────────────────────────────────

def _nearest_neighbour_fallback(request: SolveRequest, start_time: float) -> SolveResponse:
    """
    Greedy nearest-neighbour fallback used when OR-Tools is unavailable or
    returns no solution within the time limit.
    """
    now = datetime.utcnow()

    # Pick the vehicle with largest capacity that supports all required categories
    all_categories = {b.waste_category for b in request.bins}
    universal = [
        v for v in request.available_vehicles
        if all_categories.issubset(set(v.waste_categories_supported))
    ]
    pool = universal if universal else request.available_vehicles
    best_vehicle = max(pool, key=lambda v: v.max_cargo_kg)

    current_lat = best_vehicle.current_lat
    current_lng = best_vehicle.current_lng
    remaining = list(request.clusters)
    waypoints: list = []
    cumulative_weight = 0.0
    total_distance_m = 0.0
    elapsed_minutes = 0

    while remaining:
        best_cluster = None
        best_dist = float("inf")

        for cluster in remaining:
            cluster_bins = [b for b in request.bins if b.cluster_id == cluster.cluster_id]
            if not cluster_bins:
                continue
            categories = {b.waste_category for b in cluster_bins}
            if not categories.issubset(set(best_vehicle.waste_categories_supported)):
                continue
            dist = haversine_metres(current_lat, current_lng, cluster.lat, cluster.lng)
            if dist < best_dist:
                best_dist = dist
                best_cluster = cluster

        if best_cluster is None:
            break

        remaining.remove(best_cluster)
        cluster_bins = [b for b in request.bins if b.cluster_id == best_cluster.cluster_id]
        cluster_bin_ids = [b.bin_id for b in cluster_bins]
        cluster_weight = sum(b.estimated_weight_kg for b in cluster_bins)

        total_distance_m += best_dist
        elapsed_minutes += int(best_dist / 1000 / AVG_SPEED_KMH * 60)
        cumulative_weight += cluster_weight

        deadline_min = min(urgency_to_deadline_minutes(b.urgency_score) for b in cluster_bins)
        stop_duration = len(cluster_bin_ids) * SERVICE_MINUTES_PER_BIN

        waypoints.append(
            Waypoint(
                sequence=len(waypoints) + 1,
                cluster_id=best_cluster.cluster_id,
                cluster_name=best_cluster.cluster_name,
                lat=best_cluster.lat,
                lng=best_cluster.lng,
                bins=cluster_bin_ids,
                estimated_arrival_iso=(now + timedelta(minutes=elapsed_minutes)).isoformat() + "Z",
                time_window_deadline_iso=(now + timedelta(minutes=deadline_min)).isoformat() + "Z",
                cumulative_weight_kg=round(cumulative_weight, 2),
                stop_duration_minutes=stop_duration,
            )
        )

        elapsed_minutes += stop_duration
        current_lat = best_cluster.lat
        current_lng = best_cluster.lng

    # Add return-to-depot leg
    depot_dist = haversine_metres(current_lat, current_lng, request.depot.lat, request.depot.lng)
    total_distance_m += depot_dist
    elapsed_minutes += int(depot_dist / 1000 / AVG_SPEED_KMH * 60)

    return SolveResponse(
        success=True,
        job_id=request.job_id,
        method="nearest_neighbour_fallback",
        solver_time_ms=int((time.time() - start_time) * 1000),
        vehicle_id=best_vehicle.vehicle_id,
        waypoints=waypoints,
        total_distance_km=round(total_distance_m / 1000, 2),
        estimated_minutes=elapsed_minutes,
        total_weight_kg=waypoints[-1].cumulative_weight_kg if waypoints else 0.0,
        optimality_gap_pct=None,
    )


# ── Main entry point (spec §5) ────────────────────────────────────────────────

def solve(request: SolveRequest) -> SolveResponse:
    """
    Solve the CVRPTW for the given collect job.

    Tries OR-Tools first; falls back to nearest-neighbour when:
      - OR-Tools is not installed
      - Solver returns no solution within the time limit
    """
    start_time = time.time()

    validate_request(request)
    data = build_data_model(request)

    try:
        from ortools.constraint_solver import pywrapcp, routing_enums_pb2
    except ImportError:
        return _nearest_neighbour_fallback(request, start_time)

    # ── Create routing model ─────────────────────────────────────────────────
    manager = pywrapcp.RoutingIndexManager(
        len(data["locations"]),
        data["n_vehicles"],
        data["starts"],
        data["ends"],
    )
    routing = pywrapcp.RoutingModel(manager)

    # ── Distance callback ────────────────────────────────────────────────────
    def distance_callback(from_index: int, to_index: int) -> int:
        fn = manager.IndexToNode(from_index)
        tn = manager.IndexToNode(to_index)
        return data["distance_matrix"][fn][tn]

    transit_idx = routing.RegisterTransitCallback(distance_callback)
    routing.SetArcCostEvaluatorOfAllVehicles(transit_idx)

    # ── Capacity constraint ──────────────────────────────────────────────────
    def demand_callback(from_index: int) -> int:
        return data["demands"][manager.IndexToNode(from_index)]

    demand_idx = routing.RegisterUnaryTransitCallback(demand_callback)
    routing.AddDimensionWithVehicleCapacity(
        demand_idx, 0, data["vehicle_capacities"], True, "Capacity"
    )

    # ── Time window constraint ───────────────────────────────────────────────
    def time_callback(from_index: int, to_index: int) -> int:
        fn = manager.IndexToNode(from_index)
        tn = manager.IndexToNode(to_index)
        return data["time_matrix"][fn][tn] + data["service_times"][fn]

    time_idx = routing.RegisterTransitCallback(time_callback)
    routing.AddDimension(time_idx, 30, 480, False, "Time")
    time_dim = routing.GetDimensionOrDie("Time")

    for loc_idx, window in enumerate(data["time_windows"]):
        time_dim.CumulVar(manager.NodeToIndex(loc_idx)).SetRange(window[0], window[1])

    # ── Waste category constraint ────────────────────────────────────────────
    penalty = 1_000_000
    n_vehicles = data["n_vehicles"]

    for cluster_idx, cluster in enumerate(request.clusters):
        node_index = manager.NodeToIndex(cluster_idx + n_vehicles)
        cluster_categories = {
            b.waste_category for b in request.bins if b.cluster_id == cluster.cluster_id
        }
        for vehicle_idx, vehicle in enumerate(request.available_vehicles):
            if not cluster_categories.issubset(set(vehicle.waste_categories_supported)):
                routing.VehicleVar(node_index).RemoveValue(vehicle_idx)
        routing.AddDisjunction([node_index], penalty)

    # ── Search parameters ────────────────────────────────────────────────────
    search_params = pywrapcp.DefaultRoutingSearchParameters()
    search_params.first_solution_strategy = (
        routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    )
    search_params.local_search_metaheuristic = (
        routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    )
    search_params.time_limit.seconds = request.time_limit_seconds

    # ── Solve ────────────────────────────────────────────────────────────────
    solution = routing.SolveWithParameters(search_params)
    solver_time_ms = int((time.time() - start_time) * 1000)

    if solution:
        return extract_solution(manager, routing, solution, request, data, solver_time_ms)
    else:
        # Timed out or infeasible — use nearest-neighbour fallback
        return _nearest_neighbour_fallback(request, start_time)