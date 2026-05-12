import requests
import json

def test_solver():
    # Use 127.0.0.1 for better local resolution
    url = "http://127.0.0.1:8083/internal/route-optimizer/solve"
    
    payload = {
        "job_id": "test-job-python",
        "job_type": "emergency",
        "clusters": [
            { "cluster_id": "CL-01", "lat": 6.901, "lng": 79.861, "cluster_name": "Town Hall" }
        ],
        "bins": [
            { 
                "bin_id": "B1", 
                "cluster_id": "CL-01", 
                "lat": 6.901, 
                "lng": 79.861, 
                "waste_category": "Plastic", 
                "fill_level_pct": 95, 
                "estimated_weight_kg": 50.0, 
                "urgency_score": 95 
            }
        ],
        "available_vehicles": [
            { 
                "vehicle_id": "TRUCK-01", 
                "max_cargo_kg": 5000.0, 
                "waste_categories_supported": ["Plastic"], 
                "current_lat": 6.895, 
                "current_lng": 79.855 
            }
        ],
        "depot": { "lat": 6.912, "lng": 79.870 },
        "time_limit_seconds": 5
    }

    print(f"Sending request to {url}...")
    try:
        response = requests.post(url, json=payload, timeout=10)
        
        if response.status_code != 200:
            print(f"FAILED (Status: {response.status_code})")
            print(f"Response: {response.text}")
            return

        data = response.json()
        print("SUCCESS!")
        print(f"Vehicle Assigned: {data['vehicle_id']}")
        print(f"Total Distance: {data['total_distance_km']} km")
        print(f"Estimated Time: {data['estimated_minutes']} minutes")
        
        if data.get('polyline'):
            print(f"Polyline Generated (Length: {len(data['polyline'])} chars)")
        else:
            print("No polyline generated")
            
        print("Waypoints:")
        for wp in data['waypoints']:
            print(f"  {wp['sequence']}. {wp['cluster_name']} (Bins: {wp['bins']})")
            
    except requests.exceptions.ConnectionError:
        print("ERROR: Could not connect to the server at localhost:8083. Is the Docker container running?")
    except Exception as e:
        print(f"ERROR: {str(e)}")

if __name__ == "__main__":
    test_solver()
