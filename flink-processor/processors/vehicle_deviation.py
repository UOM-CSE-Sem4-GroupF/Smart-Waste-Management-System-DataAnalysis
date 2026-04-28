import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from processors.vehicle_position import ValidationError as VehiclePositionValidationError
from processors.vehicle_position import VehiclePositionProcessor
from route_store import RoutePlan, RouteStore


class ValidationError(Exception):
    pass


@dataclass
class DeviationState:
    start_time: datetime
    latest_distance_m: float
    alert_sent: bool = False


class VehicleDeviationProcessor:
    DEVIATION_THRESHOLD_M = 500.0
    ALERT_DURATION_SECONDS = 120

    def __init__(self, route_store: RouteStore) -> None:
        self.route_store = route_store
        self._gps_parser = VehiclePositionProcessor()
        self._state: Dict[Tuple[str, str], DeviationState] = {}
        self._route_cache: Dict[Tuple[str, str], Optional[RoutePlan]] = {}

    @staticmethod
    def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        radius_m = 6371000.0
        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        delta_phi = math.radians(lat2 - lat1)
        delta_lambda = math.radians(lon2 - lon1)
        a = (
            math.sin(delta_phi / 2.0) ** 2
            + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2.0) ** 2
        )
        return 2.0 * radius_m * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))

    @classmethod
    def _coerce_timestamp(cls, value: Any) -> datetime:
        if isinstance(value, datetime):
            return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        if isinstance(value, str):
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)
        raise ValidationError("timestamp must be valid")

    @staticmethod
    def _normalise_point(waypoint: Dict[str, Any]) -> Tuple[float, float]:
        latitude = waypoint.get("latitude", waypoint.get("lat"))
        longitude = waypoint.get("longitude", waypoint.get("lng", waypoint.get("lon")))
        if not isinstance(latitude, (int, float)) or not isinstance(longitude, (int, float)):
            raise ValidationError("Route waypoint must contain numeric latitude/longitude")
        return float(latitude), float(longitude)

    def parse_gps_event(self, raw_message: Any) -> Dict[str, Any]:
        try:
            return self._gps_parser.parse_kafka_message(raw_message)
        except VehiclePositionValidationError as exc:
            raise ValidationError(str(exc)) from exc

    def load_route_plan(self, vehicle_id: str, job_id: Optional[str]) -> Optional[RoutePlan]:
        cache_key = (vehicle_id, job_id or "")
        if cache_key not in self._route_cache:
            self._route_cache[cache_key] = self.route_store.load_route_plan(vehicle_id, job_id)
        return self._route_cache[cache_key]

    def nearest_waypoint_distance(self, gps_event: Dict[str, Any], route_plan: RoutePlan) -> float:
        distances = []
        for waypoint in route_plan.waypoints:
            waypoint_lat, waypoint_lon = self._normalise_point(waypoint)
            distances.append(
                self.haversine(
                    float(gps_event["latitude"]),
                    float(gps_event["longitude"]),
                    waypoint_lat,
                    waypoint_lon,
                )
            )
        if not distances:
            raise ValidationError("Route plan has no usable waypoints")
        return min(distances)

    def _state_key(self, vehicle_id: str, job_id: Optional[str]) -> Tuple[str, str]:
        return (vehicle_id, job_id or "")

    def _build_alert(self, gps_event: Dict[str, Any], deviation_m: float, duration_s: int) -> Dict[str, Any]:
        timestamp = gps_event["timestamp"]
        if not isinstance(timestamp, datetime):
            timestamp = self._coerce_timestamp(timestamp)
        return {
            "vehicle_id": gps_event["vehicle_id"],
            "job_id": gps_event.get("job_id"),
            "deviation_m": round(float(deviation_m), 1),
            "duration_s": int(duration_s),
            "timestamp": timestamp.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        }

    def process(self, raw_message: Any) -> Optional[Dict[str, Any]]:
        gps_event = self.parse_gps_event(raw_message)
        route_plan = self.load_route_plan(gps_event["vehicle_id"], gps_event.get("job_id"))
        if route_plan is None:
            return None

        nearest_distance_m = self.nearest_waypoint_distance(gps_event, route_plan)
        key = self._state_key(gps_event["vehicle_id"], gps_event.get("job_id"))
        event_ts = gps_event["timestamp"]
        if not isinstance(event_ts, datetime):
            event_ts = self._coerce_timestamp(event_ts)

        if nearest_distance_m <= self.DEVIATION_THRESHOLD_M:
            self._state.pop(key, None)
            return None

        state = self._state.get(key)
        if state is None:
            self._state[key] = DeviationState(
                start_time=event_ts,
                latest_distance_m=nearest_distance_m,
                alert_sent=False,
            )
            return None

        duration_s = int((event_ts - state.start_time).total_seconds())
        state.latest_distance_m = nearest_distance_m

        if duration_s > self.ALERT_DURATION_SECONDS and not state.alert_sent:
            state.alert_sent = True
            return self._build_alert(gps_event, nearest_distance_m, duration_s)

        return None