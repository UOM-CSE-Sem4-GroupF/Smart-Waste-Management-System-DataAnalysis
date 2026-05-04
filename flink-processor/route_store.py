import json
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import psycopg2
from psycopg2 import pool

from config import Settings


@dataclass(frozen=True)
class RoutePlan:
    route_plan_id: str
    vehicle_id: str
    job_id: Optional[str]
    waypoints: List[Dict[str, Any]]


class RouteStoreError(Exception):
    pass


class RouteStore:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        try:
            self._pool: Optional[pool.SimpleConnectionPool] = pool.SimpleConnectionPool(
                minconn=1,
                maxconn=5,
                host=self.settings.postgres_host,
                port=self.settings.postgres_port,
                database=self.settings.postgres_db,
                user=self.settings.postgres_user,
                password=self.settings.postgres_password,
            )
        except psycopg2.Error as exc:
            raise RouteStoreError(f"Failed to connect to PostgreSQL: {exc}") from exc

    @staticmethod
    def _normalize_job_id(job_id: Optional[str]) -> Optional[str]:
        if job_id is None:
            return None
        try:
            return str(uuid.UUID(str(job_id)))
        except (ValueError, TypeError, AttributeError):
            return None

    @staticmethod
    def _extract_waypoints(raw_waypoints: Any) -> List[Dict[str, Any]]:
        if isinstance(raw_waypoints, str):
            raw_waypoints = json.loads(raw_waypoints)

        if isinstance(raw_waypoints, dict):
            for key in ("waypoints", "route", "points", "stops"):
                if isinstance(raw_waypoints.get(key), list):
                    raw_waypoints = raw_waypoints.get(key)
                    break

        if not isinstance(raw_waypoints, list):
            raise RouteStoreError(f"Unsupported route_plans.waypoints format: {type(raw_waypoints)}")

        waypoints: List[Dict[str, Any]] = []
        for waypoint in raw_waypoints:
            if not isinstance(waypoint, dict):
                continue
            latitude = waypoint.get("latitude", waypoint.get("lat"))
            longitude = waypoint.get("longitude", waypoint.get("lng", waypoint.get("lon")))
            if isinstance(latitude, (int, float)) and isinstance(longitude, (int, float)):
                waypoints.append({"latitude": float(latitude), "longitude": float(longitude)})

        if not waypoints:
            raise RouteStoreError("No valid waypoints found in route_plans.waypoints")
        return waypoints

    def load_route_plan(self, vehicle_id: str, job_id: Optional[str]) -> Optional[RoutePlan]:
        conn = self._pool.getconn()
        try:
            normalized_job_id = self._normalize_job_id(job_id)
            with conn.cursor() as cur:
                if normalized_job_id is not None:
                    cur.execute(
                        """
                        SELECT id, vehicle_id, job_id, waypoints
                        FROM route_plans
                        WHERE vehicle_id = %s AND job_id = %s::uuid
                        ORDER BY created_at DESC
                        LIMIT 1
                        """,
                        (vehicle_id, normalized_job_id),
                    )
                else:
                    cur.execute(
                        """
                        SELECT id, vehicle_id, job_id, waypoints
                        FROM route_plans
                        WHERE vehicle_id = %s
                        ORDER BY created_at DESC
                        LIMIT 1
                        """,
                        (vehicle_id,),
                    )
                row = cur.fetchone()
                if row is None:
                    return None

            route_plan_id, loaded_vehicle_id, loaded_job_id, waypoints = row
            return RoutePlan(
                route_plan_id=str(route_plan_id),
                vehicle_id=str(loaded_vehicle_id),
                job_id=str(loaded_job_id) if loaded_job_id is not None else None,
                waypoints=self._extract_waypoints(waypoints),
            )
        except Exception as exc:
            raise RouteStoreError(f"Failed to load route plan: {exc}") from exc
        finally:
            self._pool.putconn(conn)

    def close(self) -> None:
        if self._pool is not None:
            self._pool.closeall()