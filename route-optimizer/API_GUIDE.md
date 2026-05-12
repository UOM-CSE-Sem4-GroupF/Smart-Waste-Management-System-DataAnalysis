# SWMS Route Optimizer API Guide

This guide provides the API blueprints for the Route Optimizer service. 

> [!WARNING] 
> **Internal Service Only**: This API is designed to be called exclusively by the **Scheduler** or **Orchestrator** services. It is **NOT** exposed to the frontend via the Kong Gateway (all `/internal/*` routes are blocked externally).

---

## **0. Service Connectivity**

The Route Optimizer service is accessible via the following paths:

| Caller Environment | Base URL | Usage |
| :--- | :--- | :--- |
| **Local Development** | `http://localhost:8083` | Local testing |
| **Deployment (K8s DNS)** | `http://route-optimizer-base-service.waste-dev.svc.cluster.local:8083` | Internal backend-to-backend communication |

---

## **1. Endpoints**

### **1.1 Solve Route**
`POST /internal/route-optimizer/solve`

Calculates the optimal Capacitated Vehicle Routing Problem with Time Windows (CVRPTW) for a given set of bins, clusters, and available vehicles.

#### **Request Body**
```json
{
  "job_id": "JOB-1001",
  "job_type": "emergency",
  "clusters": [
    {
      "cluster_id": "CL-001",
      "lat": 6.901,
      "lng": 79.861,
      "cluster_name": "Main Street Cluster"
    }
  ],
  "bins": [
    {
      "bin_id": "BIN-101",
      "cluster_id": "CL-001",
      "lat": 6.901,
      "lng": 79.861,
      "waste_category": "food_waste",
      "fill_level_pct": 95.5,
      "estimated_weight_kg": 45.0,
      "urgency_score": 95,
      "predicted_full_at": "2024-05-12T10:00:00Z"
    }
  ],
  "available_vehicles": [
    {
      "vehicle_id": "LORRY-01",
      "max_cargo_kg": 5000.0,
      "waste_categories_supported": ["food_waste", "general"],
      "current_lat": 6.890,
      "current_lng": 79.850
    }
  ],
  "depot": {
    "lat": 6.930,
    "lng": 79.840
  },
  "time_limit_seconds": 30
}
```

#### **Response (200 OK)**
```json
{
  "success": true,
  "job_id": "JOB-1001",
  "method": "or_tools",
  "solver_time_ms": 1250,
  "vehicle_id": "LORRY-01",
  "total_distance_km": 12.5,
  "estimated_minutes": 45,
  "total_weight_kg": 45.0,
  "optimality_gap_pct": 0.0,
  "polyline": "encoded_polyline_string_for_map_rendering",
  "waypoints": [
    {
      "sequence": 1,
      "cluster_id": "CL-001",
      "cluster_name": "Main Street Cluster",
      "lat": 6.901,
      "lng": 79.861,
      "bins": ["BIN-101"],
      "estimated_arrival_iso": "2024-05-12T08:15:00Z",
      "time_window_deadline_iso": "2024-05-12T10:00:00Z",
      "cumulative_weight_kg": 45.0,
      "stop_duration_minutes": 5
    }
  ]
}
```

#### **Error Responses**
- **400 Bad Request**: Invalid request format or unsupported conditions.
  ```json
  { "error": "INVALID_REQUEST", "detail": "Vehicle LORRY-01 does not support category e_waste" }
  ```
- **422 Unprocessable Entity**: No feasible solution found (e.g., total weight exceeds all available vehicles).
  ```json
  { "error": "NO_FEASIBLE_SOLUTION", "detail": "Total weight 15000kg exceeds max fleet capacity 5000kg" }
  ```
- **504 Gateway Timeout**: The solver could not find any solution within the `time_limit_seconds`.
  ```json
  { "error": "SOLVER_TIMEOUT", "detail": "OR-Tools timed out after 30s" }
  ```

---

## **2. Operations & Health**

### **2.1 Health Check**
`GET /health`
- **Response**:
  ```json
  { "status": "ok", "service": "route-optimizer", "version": "1.0.0" }
  ```

### **2.2 Readiness Check**
`GET /ready`
- **Response**:
  ```json
  { "status": "ready", "service": "route-optimizer", "version": "1.0.0" }
  ```
