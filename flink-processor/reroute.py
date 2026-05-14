import math
from datetime import datetime, timezone
from typing import Dict, List, Tuple


def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius_m = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0) ** 2
    return 2.0 * radius_m * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))


def compute_reroute(gps_event: Dict[str, any], route_plan) -> List[Dict[str, float]]:
    """Compute a simple nearest-neighbour reordering of remaining waypoints.

    gps_event: parsed gps event containing latitude/longitude
    route_plan: RoutePlan with .waypoints list of dicts containing latitude/longitude
    Returns a list of waypoints in the suggested order (each waypoint is {'latitude', 'longitude'})
    """
    try:
        cur_lat = float(gps_event["latitude"])
        cur_lon = float(gps_event["longitude"])
    except Exception:
        # fallback if gps_event has different keys
        cur_lat = float(gps_event.get("lat", 0.0))
        cur_lon = float(gps_event.get("lng", 0.0))

    remaining = [wp.copy() for wp in route_plan.waypoints]
    ordered: List[Dict[str, float]] = []
    cur = (cur_lat, cur_lon)
    while remaining:
        nearest_idx = 0
        nearest_dist = _haversine(cur[0], cur[1], float(remaining[0]["latitude"]), float(remaining[0]["longitude"]))
        for i in range(1, len(remaining)):
            d = _haversine(cur[0], cur[1], float(remaining[i]["latitude"]), float(remaining[i]["longitude"]))
            if d < nearest_dist:
                nearest_dist = d
                nearest_idx = i
        next_wp = remaining.pop(nearest_idx)
        ordered.append({"latitude": float(next_wp["latitude"]), "longitude": float(next_wp["longitude"])})
        cur = (ordered[-1]["latitude"], ordered[-1]["longitude"])    

    return ordered


def build_reroute_event(alert: Dict[str, any], gps_event: Dict[str, any], route_plan, new_waypoints: List[Dict[str, float]]) -> Dict[str, any]:
    return {
        "version": "1.0",
        "source_service": "flink-processor-reroute",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": {
            "vehicle_id": alert.get("vehicle_id"),
            "job_id": alert.get("job_id"),
            "route_plan_id": getattr(route_plan, "route_plan_id", None),
            "original_deviation": alert,
            "current_position": {"latitude": gps_event.get("latitude"), "longitude": gps_event.get("longitude")},
            "new_route": new_waypoints,
        },
    }
