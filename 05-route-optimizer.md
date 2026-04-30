# Technical Specification — OR-Tools Route Optimizer
**Owner:** F2  
**Repo:** group-f-data/route-optimizer  
**Version:** 1.0  
**Stack:** Python 3.11 · FastAPI · OR-Tools · Pydantic

---

## 1. Purpose

The route optimizer solves the Vehicle Routing Problem (VRP) for collection jobs. It takes a set of bin clusters, available vehicles, and constraints — and returns the optimal ordered route for the assigned vehicle.

It is a pure computational service. It has no database of its own, publishes nothing to Kafka, and holds no state between requests. It is called exclusively by the scheduler service via internal REST API.

---

## 2. Context in the system

```
Scheduler service ──► POST /internal/route-optimizer/solve ──► Route response
                       (sync request/response — no Kafka)
```

OR-Tools was removed from Kafka because:
- This is a request/response interaction — orchestrator/scheduler needs the result immediately
- No other service consumes OR-Tools output except scheduler
- Kafka would require correlation IDs to match responses to requests
- REST is simpler, more traceable, and fits the sync nature of the interaction

---

## 3. Responsibilities

- Accept route optimization requests from scheduler
- Solve CVRPTW (Capacitated VRP with Time Windows)
- Respect vehicle weight limits
- Respect waste category constraints per vehicle
- Enforce time windows derived from bin urgency scores
- Return ordered waypoints with estimated arrival times
- Fall back gracefully when time limit expires

---

## 4. API

### POST /internal/route-optimizer/solve

**Request body:**

```python
class SolveRequest(BaseModel):
    job_id: str
    job_type: str                    # routine | emergency

    clusters: List[ClusterInput]
    bins: List[BinInput]
    available_vehicles: List[VehicleInput]

    depot: DepotInput
    time_limit_seconds: int = 30     # solver time limit


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
    predicted_full_at: Optional[str]  # ISO 8601 or null


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
```

**Response body:**

```python
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
    optimality_gap_pct: Optional[float]


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
```

**Error responses:**

```python
# 400 — invalid request
{ "error": "INVALID_REQUEST", "detail": "No vehicles support waste category: glass" }

# 422 — no feasible solution
{ "error": "NO_FEASIBLE_SOLUTION",
  "detail": "Total weight 15,200 kg exceeds all available vehicles" }

# 504 — timeout with no solution
{ "error": "SOLVER_TIMEOUT", "detail": "No solution found within time limit" }
# Note: scheduler should use nearest-neighbour fallback in this case
```

---

## 5. VRP model definition

```python
# optimizer/vrp_solver.py

from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp
from datetime import datetime
import math

def solve(request: SolveRequest) -> SolveResponse:

    start_time = time.time()

    # ── BUILD DATA MODEL ────────────────────────────────────
    data = build_data_model(request)

    # ── CREATE ROUTING MODEL ────────────────────────────────
    manager = pywrapcp.RoutingIndexManager(
        len(data['locations']),
        len(data['vehicles']),
        data['starts'],       # each vehicle starts at its current location
        data['ends']          # all vehicles end at depot
    )

    routing = pywrapcp.RoutingModel(manager)


    # ── DISTANCE CALLBACK ───────────────────────────────────
    def distance_callback(from_index, to_index):
        from_node = manager.IndexToNode(from_index)
        to_node   = manager.IndexToNode(to_index)
        return data['distance_matrix'][from_node][to_node]
        # distance in metres (integer)

    transit_callback_index = routing.RegisterTransitCallback(distance_callback)
    routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)


    # ── CAPACITY CONSTRAINT ─────────────────────────────────
    def demand_callback(from_index):
        node = manager.IndexToNode(from_index)
        return data['demands'][node]
        # weight in kg × 100 (integer arithmetic)

    demand_index = routing.RegisterUnaryTransitCallback(demand_callback)
    routing.AddDimensionWithVehicleCapacity(
        demand_index,
        0,                           # no slack
        data['vehicle_capacities'],  # max per vehicle (kg × 100)
        True,
        'Capacity'
    )


    # ── TIME WINDOW CONSTRAINT ──────────────────────────────
    def time_callback(from_index, to_index):
        from_node = manager.IndexToNode(from_index)
        to_node   = manager.IndexToNode(to_index)
        travel_time = data['time_matrix'][from_node][to_node]
        service_time = data['service_times'][from_node]
        return travel_time + service_time

    time_index = routing.RegisterTransitCallback(time_callback)
    routing.AddDimension(
        time_index,
        30,    # allow 30 minutes waiting at location
        480,   # max route duration: 8 hours
        False,
        'Time'
    )

    time_dimension = routing.GetDimensionOrDie('Time')

    for location_idx, window in enumerate(data['time_windows']):
        index = manager.NodeToIndex(location_idx)
        time_dimension.CumulVar(index).SetRange(window[0], window[1])


    # ── WASTE CATEGORY CONSTRAINT ───────────────────────────
    # Implemented via penalties: if vehicle doesn't support bin's
    # waste category, add a very high penalty for that arc

    penalty = 1_000_000  # effectively forces category matching
    for bin_idx, bin_data in enumerate(request.bins):
        node_index = manager.NodeToIndex(bin_idx + len(data['vehicle_starts']))
        for vehicle_idx, vehicle in enumerate(request.available_vehicles):
            if bin_data.waste_category not in vehicle.waste_categories_supported:
                routing.VehicleVar(node_index).RemoveValue(vehicle_idx)
        routing.AddDisjunction([node_index], penalty)
        # all bins must be visited (high penalty if skipped)


    # ── SEARCH PARAMETERS ───────────────────────────────────
    search_params = pywrapcp.DefaultRoutingSearchParameters()

    search_params.first_solution_strategy = (
        routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    )
    search_params.local_search_metaheuristic = (
        routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    )
    search_params.time_limit.seconds = request.time_limit_seconds


    # ── SOLVE ────────────────────────────────────────────────
    solution = routing.SolveWithParameters(search_params)

    solver_time_ms = int((time.time() - start_time) * 1000)

    if solution:
        return extract_solution(
            manager, routing, solution, request, data, solver_time_ms
        )
    else:
        raise NoFeasibleSolutionError("OR-Tools found no solution")
```

---

## 6. Data model builder

```python
def build_data_model(request: SolveRequest) -> dict:
    data = {}

    # Locations:
    # [0..N-1]   = vehicle start positions
    # [N..N+M-1] = bin cluster locations
    # [last]     = depot (end point for all vehicles)

    vehicle_starts = [
        {'lat': v.current_lat, 'lng': v.current_lng}
        for v in request.available_vehicles
    ]
    cluster_locations = [
        {'lat': c.lat, 'lng': c.lng}
        for c in request.clusters
    ]
    depot = {'lat': request.depot.lat, 'lng': request.depot.lng}

    all_locations = vehicle_starts + cluster_locations + [depot]
    n = len(all_locations)
    n_vehicles = len(request.available_vehicles)
    depot_index = n - 1

    data['locations'] = all_locations
    data['starts'] = list(range(n_vehicles))
    data['ends'] = [depot_index] * n_vehicles


    # Distance matrix (metres, integer)
    data['distance_matrix'] = build_distance_matrix(all_locations)

    # Time matrix (minutes, integer)
    # Assume 30 km/h average urban speed
    data['time_matrix'] = [
        [int(data['distance_matrix'][i][j] / 1000 / 30 * 60)
         for j in range(n)]
        for i in range(n)
    ]

    # Service times (minutes per stop)
    # Vehicle start positions: 0 minutes
    # Bin clusters: 5 minutes per bin at cluster
    # Depot: 0 minutes
    service_times = [0] * n_vehicles

    for cluster in request.clusters:
        bins_at_cluster = [b for b in request.bins
                           if b.cluster_id == cluster.cluster_id]
        service_times.append(len(bins_at_cluster) * 5)

    service_times.append(0)  # depot
    data['service_times'] = service_times


    # Demands (weight in kg × 100 for integer arithmetic)
    demands = [0] * n_vehicles  # vehicle start positions have 0 demand

    for cluster in request.clusters:
        cluster_weight = sum(
            b.estimated_weight_kg for b in request.bins
            if b.cluster_id == cluster.cluster_id
        )
        demands.append(int(cluster_weight * 100))

    demands.append(0)  # depot
    data['demands'] = demands


    # Vehicle capacities (kg × 100)
    data['vehicle_capacities'] = [
        int(v.max_cargo_kg * 100)
        for v in request.available_vehicles
    ]


    # Time windows (minutes from now)
    # Vehicle start positions: [0, 480]
    # Each cluster: [0, deadline_minutes]
    # Depot: [0, 480]

    now = datetime.utcnow()
    time_windows = [[0, 480]] * n_vehicles

    for cluster in request.clusters:
        # Deadline = tightest time window among bins at this cluster
        cluster_bins = [b for b in request.bins
                        if b.cluster_id == cluster.cluster_id]

        deadlines = []
        for bin_data in cluster_bins:
            deadline_min = urgency_to_deadline_minutes(bin_data.urgency_score)
            deadlines.append(deadline_min)

        tightest = min(deadlines) if deadlines else 480
        time_windows.append([0, tightest])

    time_windows.append([0, 480])  # depot
    data['time_windows'] = time_windows

    data['vehicles'] = request.available_vehicles
    data['clusters'] = request.clusters
    data['bins'] = request.bins

    return data


def urgency_to_deadline_minutes(urgency_score: int) -> int:
    """
    Convert urgency score to collection deadline in minutes.
    More urgent = tighter deadline.
    """
    if urgency_score >= 90:  return 60
    if urgency_score >= 80:  return 120
    if urgency_score >= 70:  return 240
    return 480  # routine bins — full shift window


def build_distance_matrix(locations: list) -> list:
    n = len(locations)
    matrix = [[0] * n for _ in range(n)]

    for i in range(n):
        for j in range(n):
            if i != j:
                matrix[i][j] = int(haversine_metres(
                    locations[i]['lat'], locations[i]['lng'],
                    locations[j]['lat'], locations[j]['lng']
                ))
    return matrix


def haversine_metres(lat1, lng1, lat2, lng2) -> float:
    R = 6_371_000  # Earth radius in metres
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = (math.sin(dlat/2)**2 +
         math.cos(math.radians(lat1)) *
         math.cos(math.radians(lat2)) *
         math.sin(dlng/2)**2)
    return R * 2 * math.asin(math.sqrt(a))
```

---

## 7. Solution extraction

```python
def extract_solution(
    manager, routing, solution, request, data, solver_time_ms
) -> SolveResponse:

    now = datetime.utcnow()
    time_dimension = routing.GetDimensionOrDie('Time')
    n_vehicles = len(request.available_vehicles)

    best_vehicle_idx = None
    best_route = []
    best_distance = float('inf')

    for vehicle_idx in range(n_vehicles):
        index = routing.Start(vehicle_idx)
        route = []
        route_distance = 0
        cumulative_weight = 0.0

        while not routing.IsEnd(index):
            node = manager.IndexToNode(index)
            cluster_node_offset = n_vehicles

            # Skip vehicle start nodes
            if node >= cluster_node_offset:
                cluster_idx = node - cluster_node_offset
                cluster = request.clusters[cluster_idx]
                cluster_bins = [b.bin_id for b in request.bins
                                if b.cluster_id == cluster.cluster_id]

                cluster_weight = sum(
                    b.estimated_weight_kg for b in request.bins
                    if b.cluster_id == cluster.cluster_id
                )
                cumulative_weight += cluster_weight

                time_var = time_dimension.CumulVar(index)
                arrival_minutes = solution.Min(time_var)
                arrival_time = now + timedelta(minutes=arrival_minutes)

                deadline_minutes = data['time_windows'][node][1]
                deadline_time = now + timedelta(minutes=deadline_minutes)

                # Service time = 5 minutes per bin
                stop_duration = len(cluster_bins) * 5

                route.append(Waypoint(
                    sequence=len(route) + 1,
                    cluster_id=cluster.cluster_id,
                    cluster_name=cluster.cluster_name,
                    lat=cluster.lat,
                    lng=cluster.lng,
                    bins=cluster_bins,
                    estimated_arrival_iso=arrival_time.isoformat() + 'Z',
                    time_window_deadline_iso=deadline_time.isoformat() + 'Z',
                    cumulative_weight_kg=round(cumulative_weight, 2),
                    stop_duration_minutes=stop_duration
                ))

            prev_index = index
            index = solution.Value(routing.NextVar(index))
            route_distance += routing.GetArcCostForVehicle(
                prev_index, index, vehicle_idx
            )

        if route and route_distance < best_distance:
            best_vehicle_idx = vehicle_idx
            best_route = route
            best_distance = route_distance

    vehicle = request.available_vehicles[best_vehicle_idx]
    total_minutes = sum(w.stop_duration_minutes for w in best_route)
    # Add travel time: distance / 30kmh in minutes
    total_minutes += int(best_distance / 1000 / 30 * 60)

    return SolveResponse(
        success=True,
        job_id=request.job_id,
        method='or_tools',
        solver_time_ms=solver_time_ms,
        vehicle_id=vehicle.vehicle_id,
        waypoints=best_route,
        total_distance_km=round(best_distance / 1000, 2),
        estimated_minutes=total_minutes,
        total_weight_kg=best_route[-1].cumulative_weight_kg if best_route else 0,
        optimality_gap_pct=None  # OR-Tools doesn't expose this directly
    )
```

---

## 8. Health endpoint

```
GET /health
Response: { "status": "ok", "service": "route-optimizer", "version": "1.0.0" }
No auth required
```

---

## 9. Acceptance criteria

```
[ ] API: POST /solve returns waypoints for valid request
[ ] API: waypoints are in correct visit order
[ ] API: cumulative_weight_kg never exceeds vehicle max_cargo_kg
[ ] API: bins from incompatible waste categories not assigned to vehicle
[ ] API: urgent bins (score >= 90) assigned earlier in route
[ ] API: returns 422 when total weight exceeds all available vehicles
[ ] API: responds within 35 seconds (30s solver + 5s buffer)
[ ] API: estimated_arrival_iso is a valid ISO 8601 datetime
[ ] Performance: solves 20-cluster problem in < 30 seconds
[ ] Performance: solves 5-cluster problem in < 5 seconds
[ ] Data: weight calculation uses integer arithmetic (no floating point errors)
[ ] Data: distance matrix uses Haversine formula
[ ] Data: time windows correctly derived from urgency scores
```
