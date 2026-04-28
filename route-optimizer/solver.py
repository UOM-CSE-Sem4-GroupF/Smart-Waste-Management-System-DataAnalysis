from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Iterable

from models import BinCandidate, EmergencyOptimizationSnapshot, OptimizationPlan, RouteStop, VehicleProfile, VehicleRoutePlan


AVG_SPEED_KMH = 25.0
SERVICE_MINUTES_PER_STOP = 3
DROP_PENALTY = 1_000_000


@dataclass(frozen=True)
class _VehicleState:
    vehicle: VehicleProfile
    used_capacity_kg: float
    last_lat: float
    last_lng: float
    stops: tuple[RouteStop, ...]
    distance_km: float
    elapsed_minutes: int


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius_km = 6371.0
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)

    a = (
        math.sin(delta_lat / 2.0) ** 2
        + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2.0) ** 2
    )
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    return radius_km * c


def _travel_minutes(distance_km: float) -> int:
    return max(1, int(round((distance_km / AVG_SPEED_KMH) * 60.0)))


def _supports_category(vehicle: VehicleProfile, waste_category: str) -> bool:
    if not vehicle.waste_categories_supported:
        return True
    return waste_category in vehicle.waste_categories_supported


def _urgency_window_minutes(urgency_score: int) -> tuple[int, int]:
    if urgency_score >= 90:
        return (0, 60)
    if urgency_score >= 80:
        return (0, 120)
    if urgency_score >= 70:
        return (0, 240)
    return (0, 480)


def _compute_depot(snapshot: EmergencyOptimizationSnapshot) -> tuple[float, float]:
    if not snapshot.urgent_bins:
        return (0.0, 0.0)

    lat_sum = sum(bin_candidate.lat for bin_candidate in snapshot.urgent_bins)
    lng_sum = sum(bin_candidate.lng for bin_candidate in snapshot.urgent_bins)
    total = float(len(snapshot.urgent_bins))
    return (lat_sum / total, lng_sum / total)


def solve_emergency_routes(snapshot: EmergencyOptimizationSnapshot, use_ortools: bool = True) -> OptimizationPlan:
    if not snapshot.urgent_bins or not snapshot.vehicles:
        return OptimizationPlan(
            zone_id=snapshot.zone_id,
            solver_used="none",
            routes=(),
            unassigned_bins=tuple(bin_candidate.bin_id for bin_candidate in snapshot.urgent_bins),
        )

    if use_ortools:
        plan = _solve_with_ortools(snapshot)
        if plan is not None:
            return plan

    return _solve_with_greedy_fallback(snapshot)


def _solve_with_ortools(snapshot: EmergencyOptimizationSnapshot) -> OptimizationPlan | None:
    try:
        from ortools.constraint_solver import pywrapcp, routing_enums_pb2
    except Exception:
        return None

    depot = _compute_depot(snapshot)
    nodes = [
        {
            "bin_id": "DEPOT",
            "lat": depot[0],
            "lng": depot[1],
            "demand": 0,
            "urgency": 0,
        }
    ]
    for candidate in snapshot.urgent_bins:
        nodes.append(
            {
                "bin_id": candidate.bin_id,
                "lat": candidate.lat,
                "lng": candidate.lng,
                "demand": int(round(candidate.estimated_weight_kg * 10.0)),
                "urgency": candidate.urgency_score,
                "waste_category": candidate.waste_category,
            }
        )

    vehicle_count = len(snapshot.vehicles)
    manager = pywrapcp.RoutingIndexManager(len(nodes), vehicle_count, 0)
    routing = pywrapcp.RoutingModel(manager)

    distance_matrix = _build_distance_matrix(nodes)

    def transit_callback(from_index: int, to_index: int) -> int:
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)
        return distance_matrix[from_node][to_node]

    transit_index = routing.RegisterTransitCallback(transit_callback)
    routing.SetArcCostEvaluatorOfAllVehicles(transit_index)

    def demand_callback(from_index: int) -> int:
        from_node = manager.IndexToNode(from_index)
        return int(nodes[from_node]["demand"])

    demand_index = routing.RegisterUnaryTransitCallback(demand_callback)
    capacities = [int(round(vehicle.max_cargo_kg * 10.0)) for vehicle in snapshot.vehicles]
    routing.AddDimensionWithVehicleCapacity(demand_index, 0, capacities, True, "Capacity")

    def time_callback(from_index: int, to_index: int) -> int:
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)
        distance_m = distance_matrix[from_node][to_node]
        distance_km = float(distance_m) / 1000.0
        return _travel_minutes(distance_km) + SERVICE_MINUTES_PER_STOP

    time_index = routing.RegisterTransitCallback(time_callback)
    routing.AddDimension(time_index, 240, 24 * 60, False, "Time")
    time_dimension = routing.GetDimensionOrDie("Time")

    for node_idx in range(1, len(nodes)):
        urgency = int(nodes[node_idx]["urgency"])
        start, end = _urgency_window_minutes(urgency)
        index = manager.NodeToIndex(node_idx)
        time_dimension.CumulVar(index).SetRange(start, end)

        category = str(nodes[node_idx]["waste_category"])
        allowed_vehicle_indexes = [
            vehicle_index
            for vehicle_index, vehicle in enumerate(snapshot.vehicles)
            if _supports_category(vehicle, category)
        ]
        if not allowed_vehicle_indexes:
            routing.AddDisjunction([index], DROP_PENALTY)
            continue

        vehicle_var = routing.VehicleVar(index)
        for vehicle_index in range(vehicle_count):
            if vehicle_index not in allowed_vehicle_indexes:
                vehicle_var.RemoveValue(vehicle_index)

        routing.AddDisjunction([index], DROP_PENALTY)

    search = pywrapcp.DefaultRoutingSearchParameters()
    search.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PARALLEL_CHEAPEST_INSERTION
    search.local_search_metaheuristic = routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    search.time_limit.seconds = 15

    solution = routing.SolveWithParameters(search)
    if solution is None:
        return None

    routes: list[VehicleRoutePlan] = []
    assigned = set()
    for vehicle_index, vehicle in enumerate(snapshot.vehicles):
        index = routing.Start(vehicle_index)
        sequence = 1
        stops: list[RouteStop] = []
        distance_m = 0
        current_time = 0
        current_load = 0.0

        while not routing.IsEnd(index):
            next_index = solution.Value(routing.NextVar(index))
            node = manager.IndexToNode(index)
            next_node = manager.IndexToNode(next_index)
            if node != 0:
                bin_id = str(nodes[node]["bin_id"])
                assigned.add(bin_id)
                current_time = solution.Value(time_dimension.CumulVar(index))
                stops.append(RouteStop(sequence_number=sequence, bin_id=bin_id, estimated_arrival_min=current_time))
                current_load += float(nodes[node]["demand"]) / 10.0
                sequence += 1

            distance_m += distance_matrix[node][next_node]
            index = next_index

        if stops:
            routes.append(
                VehicleRoutePlan(
                    vehicle_id=vehicle.vehicle_id,
                    route_type="emergency",
                    stops=tuple(stops),
                    estimated_weight_kg=round(current_load, 2),
                    estimated_distance_km=round(float(distance_m) / 1000.0, 3),
                    estimated_minutes=int(current_time),
                )
            )

    unassigned = tuple(
        candidate.bin_id for candidate in snapshot.urgent_bins if candidate.bin_id not in assigned
    )

    return OptimizationPlan(
        zone_id=snapshot.zone_id,
        solver_used="ortools",
        routes=tuple(routes),
        unassigned_bins=unassigned,
    )


def _build_distance_matrix(nodes: list[dict[str, object]]) -> list[list[int]]:
    size = len(nodes)
    matrix = [[0 for _ in range(size)] for _ in range(size)]
    for i in range(size):
        for j in range(size):
            if i == j:
                continue
            matrix[i][j] = int(
                round(
                    _haversine_km(
                        float(nodes[i]["lat"]),
                        float(nodes[i]["lng"]),
                        float(nodes[j]["lat"]),
                        float(nodes[j]["lng"]),
                    )
                    * 1000.0
                )
            )
    return matrix


def _solve_with_greedy_fallback(snapshot: EmergencyOptimizationSnapshot) -> OptimizationPlan:
    depot_lat, depot_lng = _compute_depot(snapshot)
    vehicles = sorted(snapshot.vehicles, key=lambda item: item.max_cargo_kg)
    states: dict[str, _VehicleState] = {
        vehicle.vehicle_id: _VehicleState(
            vehicle=vehicle,
            used_capacity_kg=0.0,
            last_lat=depot_lat,
            last_lng=depot_lng,
            stops=(),
            distance_km=0.0,
            elapsed_minutes=0,
        )
        for vehicle in vehicles
    }

    unassigned: list[str] = []

    sorted_bins = sorted(
        snapshot.urgent_bins,
        key=lambda item: (-item.urgency_score, -item.fill_level_pct, item.bin_id),
    )

    for candidate in sorted_bins:
        feasible_states: list[_VehicleState] = []
        for vehicle in vehicles:
            if not _supports_category(vehicle, candidate.waste_category):
                continue

            state = states[vehicle.vehicle_id]
            remaining = vehicle.max_cargo_kg - state.used_capacity_kg
            if remaining + 1e-9 < candidate.estimated_weight_kg:
                continue
            feasible_states.append(state)

        if not feasible_states:
            unassigned.append(candidate.bin_id)
            continue

        chosen = min(
            feasible_states,
            key=lambda state: _haversine_km(state.last_lat, state.last_lng, candidate.lat, candidate.lng),
        )
        hop_km = _haversine_km(chosen.last_lat, chosen.last_lng, candidate.lat, candidate.lng)
        hop_minutes = _travel_minutes(hop_km)
        next_sequence = len(chosen.stops) + 1
        next_elapsed = chosen.elapsed_minutes + hop_minutes + SERVICE_MINUTES_PER_STOP

        updated = _VehicleState(
            vehicle=chosen.vehicle,
            used_capacity_kg=chosen.used_capacity_kg + candidate.estimated_weight_kg,
            last_lat=candidate.lat,
            last_lng=candidate.lng,
            stops=chosen.stops + (
                RouteStop(
                    sequence_number=next_sequence,
                    bin_id=candidate.bin_id,
                    estimated_arrival_min=next_elapsed,
                ),
            ),
            distance_km=chosen.distance_km + hop_km,
            elapsed_minutes=next_elapsed,
        )
        states[chosen.vehicle.vehicle_id] = updated

    routes: list[VehicleRoutePlan] = []
    for state in states.values():
        if not state.stops:
            continue

        routes.append(
            VehicleRoutePlan(
                vehicle_id=state.vehicle.vehicle_id,
                route_type="emergency",
                stops=state.stops,
                estimated_weight_kg=round(state.used_capacity_kg, 2),
                estimated_distance_km=round(state.distance_km, 3),
                estimated_minutes=state.elapsed_minutes,
            )
        )

    routes.sort(key=lambda route: route.vehicle_id)
    return OptimizationPlan(
        zone_id=snapshot.zone_id,
        solver_used="greedy-fallback",
        routes=tuple(routes),
        unassigned_bins=tuple(unassigned),
    )