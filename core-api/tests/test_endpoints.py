import requests
import json
import sys

BASE_URL = "http://localhost:8001/api/v1"

def test_endpoint(method, path, body=None, expected_status=200):
    url = f"{BASE_URL}{path}"
    print(f"Testing {method} {url}...", end=" ")
    try:
        if method == "GET":
            response = requests.get(url)
        elif method == "POST":
            response = requests.post(url, json=body)
        elif method == "PATCH":
            response = requests.patch(url, json=body)
        elif method == "DELETE":
            response = requests.delete(url)
        else:
            print("INVALID METHOD")
            return None

        if response.status_code == expected_status:
            print("PASSED")
            return response.json() if response.content else True
        else:
            print(f"FAILED (Status: {response.status_code})")
            if response.content:
                print(f"   Response: {response.text}")
            return None
    except Exception as e:
        print(f"ERROR: {str(e)}")
        return None

def run_tests():
    print("--- STARTING CORE API INTEGRATION TESTS (ALIGNED WITH SEED V3) ---")
    
    # 1. WASTE CATEGORIES
    cat_data = {
        "name": "test_organic_waste", # Aligned with food_waste pattern
        "avg_kg_per_litre": 0.500,
        "colour_code": "#FF0000",
        "recyclable": True,
        "special_handling": False,
        "description": "Integration test category"
    }
    cat = test_endpoint("POST", "/waste-categories", cat_data, 201)
    if not cat: return
    cat_id = cat["data"]["id"]
    
    test_endpoint("GET", f"/waste-categories/{cat_id}")
    test_endpoint("PATCH", f"/waste-categories/{cat_id}", {"avg_kg_per_litre": 0.600})
    test_endpoint("GET", "/waste-categories")

    # 2. CITY ZONES
    zone_data = {
        "name": "Test Zone – Colombo West",
        "code": "ZONE-T-01", # Aligned with ZONE-01 pattern
        "collection_day": "Friday",
        "collection_time": "1970-01-01T08:00:00.000Z"
    }
    zone = test_endpoint("POST", "/city-zones", zone_data, 201)
    if not zone: return
    zone_id = zone["data"]["id"]
    
    test_endpoint("GET", f"/city-zones/{zone_id}")
    test_endpoint("PATCH", f"/city-zones/{zone_id}", {"collection_day": "Saturday"})
    test_endpoint("GET", "/city-zones")
    
    # 3. ZONE SNAPSHOTS
    snapshot_data = {
        "zone_id": zone_id,
        "snapshot_at": "2024-05-12T10:00:00Z",
        "avg_fill_level_pct": 50.5,
        "urgent_bin_count": 5
    }
    test_endpoint("POST", "/zone-snapshots", snapshot_data, 201)
    test_endpoint("GET", f"/zone-snapshots?zone_id={zone_id}")
    
    # 4. BIN CLUSTERS
    cluster_data = {
        "id": "CLUSTER-T-001", # Aligned with CLUSTER-001 pattern
        "zone_id": zone_id,
        "name": "Test Market Cluster",
        "lat": 6.920,
        "lng": 79.860,
        "address": "99 Test St, Colombo 01",
        "cluster_type": "commercial"
    }
    cluster = test_endpoint("POST", "/clusters", cluster_data, 201)
    if not cluster: return
    cluster_id = cluster["data"]["id"]
    
    test_endpoint("GET", f"/clusters/{cluster_id}")
    test_endpoint("PATCH", f"/clusters/{cluster_id}", {"name": "Test Market Updated"})
    test_endpoint("GET", "/clusters")
    test_endpoint("GET", f"/clusters/{cluster_id}/snapshot")

    # 5. BINS
    bin_data = {
        "id": "BIN-T-001", # Aligned with BIN-001 pattern
        "cluster_id": cluster_id,
        "waste_category_id": cat_id,
        "volume_litres": 240.00,
        "depth_cm": 120,
        "address": "Test Market – Bay T"
    }
    bin_obj = test_endpoint("POST", "/bins", bin_data, 201)
    if not bin_obj: return
    bin_id = bin_obj["data"]["id"]
    
    test_endpoint("GET", f"/bins/{bin_id}")
    test_endpoint("PATCH", f"/bins/{bin_id}", {"volume_litres": 360.00})
    test_endpoint("GET", "/bins")
    
    # 6. BIN STATE (Telemetry)
    state_data = {
        "bin_id": bin_id,
        "cluster_id": cluster_id,
        "zone_id": zone_id,
        "waste_category_id": cat_id,
        "volume_litres": 240.00,
        "fill_level_pct": 82.50,
        "battery_level_pct": 91.0,
        "status": "urgent",
        "urgency_score": 81,
        "estimated_weight_kg": 40.50,
        "last_reading_at": "2024-05-12T11:00:00Z"
    }
    test_endpoint("PATCH", f"/bins/{bin_id}/state", state_data)
    test_endpoint("GET", f"/bins/{bin_id}/state")

    # 7. VEHICLES
    vehicle_data = {
        "id": "LORRY-T-01", # Aligned with LORRY-01 pattern
        "registration": "WP-TEST-1001",
        "vehicle_type": "medium",
        "max_cargo_kg": 8000.00,
        "volume_m3": 10.00,
        "status": "available"
    }
    veh = test_endpoint("POST", "/vehicles", vehicle_data, 201)
    if not veh: return
    veh_id = veh["data"]["id"]
    
    test_endpoint("GET", f"/vehicles/{veh_id}")
    test_endpoint("PATCH", f"/vehicles/{veh_id}", {"status": "dispatched"})
    test_endpoint("GET", "/vehicles")
    
    # Vehicle Categories Mapping
    test_endpoint("POST", f"/vehicles/{veh_id}/categories", {"category_id": cat_id}, 201)
    test_endpoint("DELETE", f"/vehicles/{veh_id}/categories/{cat_id}", expected_status=204)

    # 8. DEVICES
    device_data = {
        "id": "ESP32-T-0001", # Aligned with ESP32-AA-0001 pattern
        "device_type": "esp32_ultrasonic",
        "bin_id": bin_id,
        "status": "active",
        "mqtt_topic": f"sensors/bin/{bin_id}/telemetry"
    }
    dev = test_endpoint("POST", "/devices", device_data, 201)
    if not dev: return
    dev_id = dev["data"]["id"]
    
    test_endpoint("GET", f"/devices/{dev_id}")
    test_endpoint("PATCH", f"/devices/{dev_id}", {"status": "offline"})
    test_endpoint("GET", "/devices")

    # 9. ROUTE PLANS
    route_data = {
        "vehicle_id": veh_id,
        "route_type": "routine",
        "waypoints": [{"cluster_id": cluster_id, "bins": [bin_id]}],
        "total_clusters": 1,
        "total_bins": 1,
        "estimated_weight_kg": 40.5,
        "status": "planned",
        "valid_for_date": "2024-05-12T00:00:00Z"
    }
    route = test_endpoint("POST", "/route-plans", route_data, 201)
    if not route: return
    route_id = route["data"]["id"]
    
    test_endpoint("GET", f"/route-plans/{route_id}")
    test_endpoint("PATCH", f"/route-plans/{route_id}", {"status": "completed"})
    test_endpoint("GET", f"/route-plans?vehicle_id={veh_id}")

    # 10. MODEL PERFORMANCE
    perf_data = {
        "model_name": "test_fill_time_predictor",
        "model_version": "v1.0.0-test",
        "mae_hours": 2.500,
        "r_squared": 0.8800,
        "trained_at": "2024-05-12T12:00:00Z"
    }
    perf = test_endpoint("POST", "/model-performance", perf_data, 201)
    if not perf: return
    perf_id = perf["data"]["id"]
    
    test_endpoint("GET", f"/model-performance/{perf_id}")
    test_endpoint("PATCH", f"/model-performance/{perf_id}", {"promoted_to_prod": True})
    test_endpoint("GET", "/model-performance")
    test_endpoint("GET", "/model-performance/current/test_fill_time_predictor")

    # 11. METADATA
    test_endpoint("GET", f"/zones/{zone_id}/summary")

    # --- CLEANUP ---
    print("\n--- CLEANING UP TEST DATA ---")
    test_endpoint("DELETE", f"/model-performance/{perf_id}", expected_status=204)
    test_endpoint("DELETE", f"/route-plans/{route_id}", expected_status=204)
    test_endpoint("DELETE", f"/devices/{dev_id}", expected_status=204)
    test_endpoint("DELETE", f"/vehicles/{veh_id}", expected_status=204)
    test_endpoint("DELETE", f"/bins/{bin_id}", expected_status=204)
    test_endpoint("DELETE", f"/clusters/{cluster_id}", expected_status=204)
    test_endpoint("DELETE", f"/city-zones/{zone_id}", expected_status=204)
    test_endpoint("DELETE", f"/waste-categories/{cat_id}", expected_status=204)

    print("\n--- TESTS COMPLETED SUCCESSFULLY ---")

if __name__ == "__main__":
    run_tests()
