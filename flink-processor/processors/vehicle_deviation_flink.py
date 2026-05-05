import json
import logging
import math
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common import Types

from processors.vehicle_position import ValidationError as VehiclePositionValidationError
from processors.vehicle_position import VehiclePositionProcessor
from route_store import RoutePlan, RouteStore

logger = logging.getLogger(__name__)


class ValidationError(Exception):
    pass


class VehicleDeviationFlinkProcessor(KeyedProcessFunction):
    """
    PyFlink KeyedProcessFunction for stateful vehicle deviation detection (Pipeline 3).

    Keyed by vehicle_id. Maintains state about deviation start time and alerts sent.
    """

    DEVIATION_THRESHOLD_M = 500.0
    ALERT_DURATION_SECONDS = 120

    def __init__(self):
        self._route_store = None
        self._gps_parser = None
        
        # State handles
        self._start_time_state = None
        self._latest_distance_state = None
        self._alert_sent_state = None
        self._job_id_state = None
        
        # Local cache for routes to avoid hitting DB for every single ping
        # Since we are in a distributed environment, cache is per task manager instance.
        self._route_cache = {}

    def open(self, runtime_context):
        from config import load_settings
        
        settings = load_settings()
        self._route_store = RouteStore(settings)
        self._gps_parser = VehiclePositionProcessor()

        self._start_time_state = runtime_context.get_state(
            ValueStateDescriptor("start_time_ms", Types.LONG())
        )
        self._latest_distance_state = runtime_context.get_state(
            ValueStateDescriptor("latest_distance_m", Types.FLOAT())
        )
        self._alert_sent_state = runtime_context.get_state(
            ValueStateDescriptor("alert_sent", Types.BOOLEAN())
        )
        self._job_id_state = runtime_context.get_state(
            ValueStateDescriptor("job_id", Types.STRING())
        )

    def close(self):
        if self._route_store is not None:
            self._route_store.close()

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
    def _normalise_point(waypoint: Dict[str, Any]) -> tuple[float, float]:
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
        cache_key = f"{vehicle_id}_{job_id or ''}"
        if cache_key not in self._route_cache:
            self._route_cache[cache_key] = self._route_store.load_route_plan(vehicle_id, job_id)
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

    def _clear_state(self):
        self._start_time_state.clear()
        self._latest_distance_state.clear()
        self._alert_sent_state.clear()

    def process_element(self, event_json: str, ctx):
        try:
            raw_event = json.loads(event_json) if isinstance(event_json, str) else event_json
            result = self._handle(raw_event)
            if result is not None:
                yield json.dumps(result, default=str)
        except ValidationError as exc:
            logger.warning("Skipping invalid vehicle location event: %s", exc)
        except Exception:
            logger.exception("Failed processing vehicle deviation event: %.200s", event_json)

    def _handle(self, raw_message: Any) -> Optional[Dict[str, Any]]:
        gps_event = self.parse_gps_event(raw_message)
        vehicle_id = gps_event["vehicle_id"]
        job_id = gps_event.get("job_id") or ""
        
        route_plan = self.load_route_plan(vehicle_id, job_id)
        if route_plan is None:
            return None

        nearest_distance_m = self.nearest_waypoint_distance(gps_event, route_plan)
        
        event_ts = gps_event["timestamp"]
        if not isinstance(event_ts, datetime):
            event_ts = self._coerce_timestamp(event_ts)
            
        event_ms = int(event_ts.timestamp() * 1000)

        # Check if job changed, if so clear state
        prev_job_id = self._job_id_state.value()
        if prev_job_id is not None and prev_job_id != job_id:
            self._clear_state()
            
        self._job_id_state.update(job_id)

        # If on route, clear deviation state
        if nearest_distance_m <= self.DEVIATION_THRESHOLD_M:
            self._clear_state()
            return None

        # Vehicle is off route
        start_time_ms = self._start_time_state.value()
        if start_time_ms is None:
            # Start tracking deviation
            self._start_time_state.update(event_ms)
            self._latest_distance_state.update(nearest_distance_m)
            self._alert_sent_state.update(False)
            return None

        # Update latest distance
        self._latest_distance_state.update(nearest_distance_m)

        duration_s = int((event_ms - start_time_ms) / 1000)
        alert_sent = self._alert_sent_state.value() or False

        if duration_s > self.ALERT_DURATION_SECONDS and not alert_sent:
            self._alert_sent_state.update(True)
            return self._build_alert(gps_event, nearest_distance_m, duration_s)

        return None
