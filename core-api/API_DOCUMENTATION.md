# SWMS Core API Documentation

The **Core API** serves as the high-performance metadata gateway for the Smart Waste Management System (SWMS). It provides access to structural data (Zones, Clusters, Bins) and real-time urgency snapshots calculated from telemetry processing.

---

## **General Information**

- **Base URL**: `http://localhost:8001`
- **Prefix**: `/api/v1`
- **Format**: JSON
- **Documentation (Swagger)**: `http://localhost:8001/docs`

---

## **Core Endpoints**

### **1. Service Health**
Check the status of the API and its connection to the database.

- **URL**: `/health`
- **Method**: `GET`
- **Response**:
  ```json
  {
    "status": "ok",
    "service": "core-api",
    "version": "1.0.0",
    "timestamp": "2026-05-05T12:00:00Z"
  }
  ```

---

## **Metadata Endpoints**

### **2. List City Zones**
Retrieve all active waste collection zones.

- **URL**: `/api/v1/zones`
- **Method**: `GET`
- **Response**:
  ```json
  {
    "data": [
      {
        "id": 1,
        "name": "Colombo North",
        "code": "ZONE-N",
        "collection_day": "Monday",
        "collection_time": "1970-01-01T08:00:00.000Z"
      }
    ]
  }
  ```

### **3. List Waste Categories**
Retrieve all supported waste categories with density metadata.

- **URL**: `/api/v1/waste-categories`
- **Method**: `GET`
- **Response**:
  ```json
  {
    "data": [
      {
        "id": 1,
        "name": "Organic",
        "avg_kg_per_litre": 0.5,
        "colour_code": "#4CAF50",
        "recyclable": false
      }
    ]
  }
  ```

### **4. List Vehicles**
Retrieve the fleet status and supported categories for each vehicle.

- **URL**: `/api/v1/vehicles`
- **Method**: `GET`
- **Query Params**: `status`, `waste_category`
- **Response**:
  ```json
  {
    "data": [
      {
        "id": "TRUCK-001",
        "registration": "WP-ABC-1234",
        "vehicle_type": "medium",
        "max_cargo_kg": 8000,
        "status": "available",
        "waste_categories": [...]
      }
    ]
  }
  ```

---

## **Operational Endpoints (For F3 Orchestrator)**

### **5. Cluster Urgency Snapshot**
**CRITICAL**: This is the primary endpoint used by the F3 Orchestrator to plan routes. It provides a real-time roll-up of all bins within a cluster.

- **URL**: `/api/v1/clusters/:cluster_id/snapshot`
- **Method**: `GET`
- **Response**:
  ```json
  {
    "data": {
      "cluster_id": "CL-001",
      "cluster_name": "Main Street Cluster",
      "total_bins": 12,
      "urgent_bin_count": 3,
      "max_urgency_score": 85.5,
      "total_estimated_weight_kg": 450.2,
      "has_special_handling": false,
      "bins": [
        {
          "bin_id": "BIN-101",
          "waste_category": "Organic",
          "fill_level_pct": 85,
          "urgency_score": 85.5,
          "status": "urgent",
          "estimated_weight_kg": 42.5
        }
      ]
    }
  }
  ```

---

## **Data Entities**

### **Bin Status Codes**
- `normal`: Fill level < 70%
- `full`: Fill level 70-90%
- `urgent`: Fill level > 90% or overflow predicted soon.
- `critical`: Immediate collection required.

### **Pagination**
Standard list endpoints (`/bins`, `/clusters`) support pagination:
- `page`: Default `1`
- `limit`: Default `50`, Max `200`
- Response includes a `pagination` object with `total`, `pages`, etc.
