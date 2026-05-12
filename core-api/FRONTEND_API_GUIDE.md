# SWMS Full API Integration Guide

This exhaustive guide provides exact JSON request and response blueprints for **every endpoint** in the Core API (F2). 

---

## **0. Service Connectivity**

The Core API service is accessible via the following paths depending on the caller's environment:

| Caller Environment | Base URL | Usage |
| :--- | :--- | :--- |
| **Frontend / Host Machine** | `http://localhost:8001/api/v1` | Dashboard integration & manual testing |
| **Production (via Kong)** | `http://<KONG_IP>/data-analysis/api/v1` | External production access (Gateway) |
| **Deployment (K8s DNS)** | `http://core-api-base-service.waste-dev.svc.cluster.local:8001/api/v1` | Internal backend-to-backend communication |
| **Internal Port** | `8001` | Default service port |

---

## **1. Bins & Telemetry (`/bins`)**

### **1.1 List Bins**
`GET /bins`
- **Query Params**: `zone_id`, `cluster_id`, `status`, `waste_category_id`, `page`, `limit`
- **Response**:
  ```json
  {
    "data": [
      {
        "id": "BIN-101",
        "cluster_id": "CL-001",
        "waste_category_id": 1,
        "volume_litres": 120,
        "lat": 6.901,
        "lng": 79.861,
        "active": true,
        "current_state": {
          "status": "urgent",
          "fill_level_pct": 88.5,
          "urgency_score": 85
        },
        "waste_category": { "id": 1, "name": "Organic" }
      }
    ],
    "pagination": { "page": 1, "limit": 50, "total": 120, "pages": 3 }
  }
  ```

### **1.2 Get Single Bin**
`GET /bins/:bin_id`
- **Response**:
  ```json
  {
    "data": {
      "id": "BIN-101",
      "cluster_id": "CL-001",
      "volume_litres": 120,
      "lat": 6.901,
      "lng": 79.861,
      "active": true,
      "cluster": {
         "id": "CL-001",
         "name": "Main Street Cluster"
      },
      "waste_category": { "id": 1, "name": "Organic" },
      "current_state": { "status": "urgent" },
      "device": { "id": "DEV-01", "status": "active" }
    }
  }
  ```

### **1.3 Create Bin**
`POST /bins`
- **Request Body**:
  ```json
  {
    "id": "BIN-202",
    "cluster_id": "CL-005",
    "waste_category_id": 1,
    "volume_litres": 120,
    "lat": 6.905,
    "lng": 79.865
  }
  ```
- **Response (201)**: Returns the full created `Bin` object.
  ```json
  {
    "data": {
      "id": "BIN-202",
      "cluster_id": "CL-005",
      "waste_category_id": 1,
      "volume_litres": 120,
      "lat": 6.905,
      "lng": 79.865,
      "active": true
    }
  }
  ```

### **1.4 Update Bin**
`PATCH /bins/:id`
- **Request Body**:
  ```json
  {
    "volume_litres": 240,
    "active": false
  }
  ```
- **Response**:
  ```json
  {
    "data": {
      "id": "BIN-202",
      "volume_litres": 240,
      "active": false
    }
  }
  ```

### **1.5 Delete Bin**
`DELETE /bins/:id`
- **Response**: `204 No Content` (Empty Body)

### **1.6 Get Bin State**
`GET /bins/:bin_id/state`
- **Response**:
  ```json
  {
    "data": {
      "bin_id": "BIN-101",
      "fill_level_pct": 88.5,
      "urgency_score": 85,
      "status": "urgent",
      "estimated_weight_kg": 40.5,
      "last_reading_at": "2024-05-12T09:00:00Z"
    }
  }
  ```

### **1.7 Upsert Bin State**
`PATCH /bins/:bin_id/state`
- **Request Body**:
  ```json
  {
    "fill_level_pct": 92.0,
    "status": "critical",
    "urgency_score": 95,
    "estimated_weight_kg": 45.0,
    "last_reading_at": "2024-05-12T09:00:00Z"
  }
  ```
- **Response**:
  ```json
  {
    "data": {
      "bin_id": "BIN-101",
      "fill_level_pct": 92.0,
      "status": "critical",
      "urgency_score": 95
    }
  }
  ```

---

## **2. Bin Clusters (`/clusters`)**

### **2.1 List Clusters**
`GET /clusters`
- **Query Params**: `zone_id`, `page`, `limit`
- **Response**:
  ```json
  {
    "data": [
      {
        "id": "CL-001",
        "zone_id": 1,
        "name": "Main Street Cluster",
        "lat": 6.901,
        "lng": 79.861,
        "zone": { "id": 1, "name": "Colombo North" },
        "bins": [ { "id": "BIN-101", "waste_category_id": 1 } ]
      }
    ],
    "pagination": { "page": 1, "limit": 50, "total": 10, "pages": 1 }
  }
  ```

### **2.2 Get Single Cluster**
`GET /clusters/:cluster_id`
- **Response**:
  ```json
  {
    "data": {
      "id": "CL-001",
      "zone_id": 1,
      "name": "Main Street Cluster",
      "zone": { "id": 1, "name": "Colombo North" },
      "bins": [
        {
          "id": "BIN-101",
          "waste_category": { "id": 1, "name": "Organic" },
          "current_state": { "status": "urgent" }
        }
      ]
    }
  }
  ```

### **2.3 Create Cluster**
`POST /clusters`
- **Request Body**:
  ```json
  {
    "id": "CL-002",
    "zone_id": 1,
    "name": "Park Avenue",
    "lat": 6.910,
    "lng": 79.865
  }
  ```
- **Response (201)**:
  ```json
  {
    "data": {
      "id": "CL-002",
      "zone_id": 1,
      "name": "Park Avenue",
      "lat": 6.910,
      "lng": 79.865,
      "active": true
    }
  }
  ```

### **2.4 Update Cluster**
`PATCH /clusters/:id`
- **Request Body**:
  ```json
  {
    "name": "Park Avenue Renamed",
    "active": false
  }
  ```
- **Response**:
  ```json
  {
    "data": {
      "id": "CL-002",
      "name": "Park Avenue Renamed",
      "active": false
    }
  }
  ```

### **2.5 Delete Cluster**
`DELETE /clusters/:id`
- **Response**: `204 No Content` (Empty Body)

### **2.6 Cluster Snapshot (Orchestrator)**
`GET /clusters/:id/snapshot`
- **Response**:
  ```json
  {
    "data": {
      "cluster_id": "CL-001",
      "cluster_name": "Main Street Cluster",
      "lat": 6.901,
      "lng": 79.861,
      "zone_id": 1,
      "total_bins": 12,
      "urgent_bin_count": 3,
      "max_urgency_score": 95,
      "total_estimated_weight_kg": 450.2,
      "has_special_handling": false,
      "bins": [
        {
           "bin_id": "BIN-101",
           "waste_category": "Organic",
           "status": "critical",
           "urgency_score": 95,
           "estimated_weight_kg": 40.5
        }
      ]
    }
  }
  ```

---

## **3. City Zones & Summaries (`/city-zones`, `/zone-snapshots`, `/zones`)**

### **3.1 List City Zones**
`GET /city-zones`
- **Response**:
  ```json
  {
    "data": [
      {
        "id": 1,
        "name": "Colombo North",
        "code": "ZONE-N",
        "collection_day": "Monday",
        "active": true
      }
    ]
  }
  ```

### **3.2 Get Single City Zone**
`GET /city-zones/:id`
- **Response**:
  ```json
  {
    "data": {
      "id": 1,
      "name": "Colombo North",
      "code": "ZONE-N",
      "collection_day": "Monday",
      "active": true
    }
  }
  ```

### **3.3 Create City Zone**
`POST /city-zones`
- **Request Body**:
  ```json
  {
    "name": "Colombo South",
    "code": "ZONE-S",
    "collection_day": "Tuesday"
  }
  ```
- **Response (201)**:
  ```json
  {
    "data": {
      "id": 2,
      "name": "Colombo South",
      "code": "ZONE-S"
    }
  }
  ```

### **3.4 Update City Zone**
`PATCH /city-zones/:id`
- **Request Body**:
  ```json
  {
    "collection_day": "Wednesday"
  }
  ```
- **Response**:
  ```json
  {
    "data": {
      "id": 2,
      "collection_day": "Wednesday"
    }
  }
  ```

### **3.5 Delete City Zone**
`DELETE /city-zones/:id`
- **Response**: `204 No Content` (Empty Body)

### **3.6 List Zone Snapshots**
`GET /zone-snapshots?zone_id=1`
- **Response**:
  ```json
  {
    "data": [
      {
        "id": "1",
        "zone_id": 1,
        "snapshot_at": "2024-05-12T10:00:00Z",
        "avg_fill_level_pct": 45.5,
        "urgent_bin_count": 8,
        "active_bin_count": 100
      }
    ]
  }
  ```

### **3.7 Create Zone Snapshot**
`POST /zone-snapshots`
- **Request Body**:
  ```json
  {
    "zone_id": 1,
    "snapshot_at": "2024-05-12T11:00:00Z",
    "avg_fill_level_pct": 48.0,
    "urgent_bin_count": 9,
    "active_bin_count": 100
  }
  ```
- **Response (201)**: Same structure as list element.

### **3.8 Zone Summary (Dashboard)**
`GET /zones/:id/summary`
- **Response**:
  ```json
  {
    "data": {
      "zone": { "id": 1, "name": "Colombo North", "code": "ZONE-N" },
      "latest_snapshot": {
        "avg_fill_level_pct": 45.5,
        "urgent_bin_count": 8
      },
      "live_bin_status_counts": {
        "normal": 40,
        "full": 5,
        "urgent": 3
      }
    }
  }
  ```

---

## **4. Fleet Management (`/vehicles`)**

### **4.1 List Vehicles**
`GET /vehicles`
- **Query Params**: `status`, `waste_category`
- **Response**:
  ```json
  {
    "data": [
      {
        "id": "TRUCK-01",
        "registration": "WP-ABC-1234",
        "max_cargo_kg": 8000,
        "status": "available",
        "waste_categories": [
          { "category": { "id": 1, "name": "Organic" } }
        ]
      }
    ]
  }
  ```

### **4.2 Get Single Vehicle**
`GET /vehicles/:id`
- **Response**: Same format as list item.

### **4.3 Create Vehicle**
`POST /vehicles`
- **Request Body**:
  ```json
  {
    "id": "TRUCK-02",
    "registration": "WP-XYZ-9876",
    "max_cargo_kg": 10000,
    "status": "available"
  }
  ```
- **Response (201)**: Returns created vehicle.

### **4.4 Update Vehicle**
`PATCH /vehicles/:id`
- **Request Body**:
  ```json
  {
    "status": "maintenance"
  }
  ```
- **Response**: Returns updated vehicle.

### **4.5 Delete Vehicle**
`DELETE /vehicles/:id`
- **Response**: `204 No Content`

### **4.6 Assign Waste Category**
`POST /vehicles/:id/categories`
- **Request Body**:
  ```json
  { "category_id": 1 }
  ```
- **Response (201)**:
  ```json
  { "data": { "vehicle_id": "TRUCK-01", "category_id": 1 } }
  ```

### **4.7 Remove Waste Category**
`DELETE /vehicles/:id/categories/:category_id`
- **Response**: `204 No Content`

---

## **5. Waste Categories (`/waste-categories`)**

### **5.1 List Categories**
`GET /waste-categories`
- **Response**:
  ```json
  {
    "data": [
      {
        "id": 1,
        "name": "Organic",
        "avg_kg_per_litre": 0.5,
        "colour_code": "#4CAF50",
        "recyclable": false,
        "special_handling": false
      }
    ]
  }
  ```

### **5.2 Get Single Category**
`GET /waste-categories/:id`
- **Response**: Single object of the list array.

### **5.3 Create Category**
`POST /waste-categories`
- **Request Body**:
  ```json
  {
    "name": "Glass",
    "avg_kg_per_litre": 0.8,
    "colour_code": "#3F51B5",
    "recyclable": true,
    "special_handling": false
  }
  ```
- **Response (201)**: Created category.

### **5.4 Update Category**
`PATCH /waste-categories/:id`
- **Request Body**:
  ```json
  { "avg_kg_per_litre": 0.85 }
  ```
- **Response**: Updated category.

### **5.5 Delete Category**
`DELETE /waste-categories/:id`
- **Response**: `204 No Content`

---

## **6. Route Plans (`/route-plans`)**

### **6.1 List Route Plans**
`GET /route-plans`
- **Query Params**: `date` (YYYY-MM-DD), `vehicle_id`
- **Response**:
  ```json
  {
    "data": [
      {
        "id": "RP-1001",
        "vehicle_id": "TRUCK-01",
        "valid_for_date": "2024-05-12T00:00:00Z",
        "route_type": "emergency",
        "total_clusters": 5,
        "total_bins": 12,
        "status": "planned"
      }
    ]
  }
  ```

### **6.2 Get Single Route Plan**
`GET /route-plans/:id`
- **Response**:
  ```json
  {
    "data": {
      "id": "RP-1001",
      "vehicle_id": "TRUCK-01",
      "waypoints": [
        { "cluster_id": "CL-001", "bins": ["BIN-101", "BIN-102"] }
      ],
      "polyline": "encoded_polyline_string...",
      "total_clusters": 5,
      "status": "planned"
    }
  }
  ```

### **6.3 Create Route Plan**
`POST /route-plans`
- **Request Body**:
  ```json
  {
    "vehicle_id": "TRUCK-01",
    "route_type": "emergency",
    "valid_for_date": "2024-05-12T00:00:00Z",
    "waypoints": [
      { "cluster_id": "CL-001", "bins": ["BIN-101", "BIN-102"] }
    ],
    "total_clusters": 5,
    "total_bins": 12,
    "estimated_weight_kg": 450.0,
    "estimated_distance_km": 15.2,
    "polyline": "encoded_polyline_string...",
    "status": "planned"
  }
  ```
- **Response (201)**: Returns the created object.

### **6.4 Update Route Plan**
`PATCH /route-plans/:id`
- **Request Body**:
  ```json
  { "status": "completed" }
  ```
- **Response**: Returns the updated object.

### **6.5 Delete Route Plan**
`DELETE /route-plans/:id`
- **Response**: `204 No Content`

---

## **7. Model Performance (`/model-performance`)**

### **7.1 List Performance Metrics**
`GET /model-performance`
- **Query Params**: `model_name`, `promoted` (true/false)
- **Response**:
  ```json
  {
    "data": [
      {
        "id": "1",
        "model_name": "fill_time_predictor",
        "model_version": "v1.2.0",
        "mae_hours": 2.45,
        "r_squared": 0.88,
        "promoted_to_prod": true
      }
    ]
  }
  ```

### **7.2 Get Single Metric**
`GET /model-performance/:id`
- **Response**: Returns the single object.

### **7.3 Get Current Production Model**
`GET /model-performance/current/:model_name`
- **Response**:
  ```json
  {
    "data": {
      "id": "1",
      "model_name": "fill_time_predictor",
      "model_version": "v1.2.0",
      "promoted_to_prod": true,
      "promoted_at": "2024-05-12T08:30:00Z"
    }
  }
  ```

### **7.4 Record Training Run (Create)**
`POST /model-performance`
- **Request Body**:
  ```json
  {
    "model_name": "fill_time_predictor",
    "model_version": "v1.3.0",
    "trained_at": "2024-05-13T08:00:00Z",
    "mae_hours": 2.10,
    "r_squared": 0.91,
    "promoted_to_prod": false
  }
  ```
- **Response (201)**: Returns the created object.

### **7.5 Promote Model (Update)**
`PATCH /model-performance/:id`
- **Request Body**:
  ```json
  {
    "promoted_to_prod": true,
    "promoted_at": "2024-05-13T09:00:00Z"
  }
  ```
- **Response**: Returns the updated object.

### **7.6 Delete Metric**
`DELETE /model-performance/:id`
- **Response**: `204 No Content`

---

## **8. IoT Devices (`/devices`)**

### **8.1 List Devices**
`GET /devices`
- **Response**:
  ```json
  {
    "data": [
      {
        "id": "DEV-001",
        "device_type": "ultrasonic_v2",
        "bin_id": "BIN-101",
        "status": "active",
        "battery_level_pct": 85.5
      }
    ]
  }
  ```

### **8.2 Get Single Device**
`GET /devices/:id`
- **Response**: Returns the single device object.

### **8.3 Provision Device (Create)**
`POST /devices`
- **Request Body**:
  ```json
  {
    "id": "DEV-002",
    "device_type": "ultrasonic_v2",
    "bin_id": "BIN-102",
    "mqtt_topic": "swms/bins/BIN-102/telemetry",
    "sleep_interval_normal_s": 3600,
    "status": "active"
  }
  ```
- **Response (201)**: Returns the created device object.

### **8.4 Update Device**
`PATCH /devices/:id`
- **Request Body**:
  ```json
  {
    "status": "maintenance",
    "sleep_interval_normal_s": 1800
  }
  ```
- **Response**: Returns the updated device object.

### **8.5 Delete Device**
`DELETE /devices/:id`
- **Response**: `204 No Content`
