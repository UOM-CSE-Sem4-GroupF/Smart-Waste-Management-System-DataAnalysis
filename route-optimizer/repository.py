from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Iterable

from models import BinCandidate, VehicleProfile

@dataclass(frozen=True)
class EmergencySnapshotRows:
    zone_id: int
    urgent_bins: tuple[dict[str, Any], ...]
    vehicles: tuple[dict[str, Any], ...]


class RouteOptimizerRepository:
    def __init__(self, connection_factory: Callable[[], Any]) -> None:
        self._connection_factory = connection_factory

    @contextmanager
    def _connection(self):
        connection = self._connection_factory()
        try:
            yield connection
        finally:
            connection.close()

    def _dict_cursor(self, connection):
        try:
            from psycopg2.extras import RealDictCursor
        except ImportError:
            return connection.cursor()

        try:
            return connection.cursor(cursor_factory=RealDictCursor)
        except TypeError:
            return connection.cursor()

    def resolve_zone_id(self, trigger_bin_id: str) -> int:
        query = "SELECT zone_id FROM bins WHERE id = %s AND active = TRUE"
        with self._connection() as connection:
            with self._dict_cursor(connection) as cursor:
                cursor.execute(query, (trigger_bin_id,))
                row = cursor.fetchone()
                if row is None:
                    raise ValueError(f"Unable to resolve zone for bin {trigger_bin_id}")
                if isinstance(row, dict):
                    return int(row["zone_id"])
                return int(row[0])

    def load_emergency_snapshot(self, zone_id: int | None, trigger_bin_id: str, urgency_threshold: int) -> EmergencySnapshotRows:
        resolved_zone_id = zone_id if zone_id is not None else self.resolve_zone_id(trigger_bin_id)
        urgent_bins = self._load_urgent_bins(resolved_zone_id, urgency_threshold)
        vehicles = self._load_vehicles()
        return EmergencySnapshotRows(zone_id=resolved_zone_id, urgent_bins=urgent_bins, vehicles=vehicles)

    def _load_urgent_bins(self, zone_id: int, urgency_threshold: int) -> tuple[dict[str, Any], ...]:
        query = """
            SELECT
                b.id AS bin_id,
                b.zone_id,
                b.lat,
                b.lng,
                b.volume_litres,
                w.name AS waste_category,
                w.avg_kg_per_litre,
                c.fill_level_pct,
                c.status,
                c.urgency_score,
                c.estimated_weight_kg,
                c.battery_level_pct,
                c.predicted_full_at,
                c.last_reading_at
            FROM bins b
            JOIN bin_current_state c ON c.bin_id = b.id
            JOIN waste_categories w ON w.id = b.waste_category_id
            WHERE b.active = TRUE
              AND c.urgency_score >= %s
              AND b.zone_id = %s
            ORDER BY c.urgency_score DESC, c.fill_level_pct DESC, c.last_reading_at ASC, b.id ASC
        """
        with self._connection() as connection:
            with self._dict_cursor(connection) as cursor:
                cursor.execute(query, (urgency_threshold, zone_id))
                rows = cursor.fetchall()
                return tuple(dict(row) for row in rows)

    def _load_vehicles(self) -> tuple[dict[str, Any], ...]:
        query = """
            SELECT
                id,
                registration,
                max_cargo_kg,
                volume_m3,
                waste_categories_supported,
                active
            FROM vehicles
            WHERE active = TRUE
            ORDER BY max_cargo_kg ASC, id ASC
        """
        with self._connection() as connection:
            with self._dict_cursor(connection) as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
                return tuple(dict(row) for row in rows)


def normalize_snapshot(trigger: dict[str, Any], rows: EmergencySnapshotRows) -> tuple[BinCandidate, ...]:
    candidates: list[BinCandidate] = []
    for row in rows.urgent_bins:
        estimated_weight = row.get("estimated_weight_kg")
        if estimated_weight is None:
            estimated_weight = round(
                float(row["fill_level_pct"]) / 100.0
                * float(row["volume_litres"])
                * float(row["avg_kg_per_litre"]),
                2,
            )

        candidates.append(
            BinCandidate(
                bin_id=str(row["bin_id"]),
                zone_id=int(row["zone_id"]),
                lat=float(row["lat"]),
                lng=float(row["lng"]),
                waste_category=str(row["waste_category"]),
                volume_litres=float(row["volume_litres"]),
                fill_level_pct=float(row["fill_level_pct"]),
                urgency_score=int(row["urgency_score"]),
                status=str(row["status"]),
                estimated_weight_kg=float(estimated_weight),
                battery_level_pct=float(row["battery_level_pct"]) if row.get("battery_level_pct") is not None else None,
                predicted_full_at=row.get("predicted_full_at"),
                last_reading_at=row.get("last_reading_at"),
            )
        )
    return tuple(candidates)


def normalize_vehicles(rows: Iterable[dict[str, Any]]) -> tuple[VehicleProfile, ...]:
    vehicles: list[VehicleProfile] = []
    for row in rows:
        supported = row.get("waste_categories_supported") or []
        vehicles.append(
            VehicleProfile(
                vehicle_id=str(row["id"]),
                registration=str(row["registration"]),
                max_cargo_kg=float(row["max_cargo_kg"]),
                volume_m3=float(row["volume_m3"]) if row.get("volume_m3") is not None else None,
                waste_categories_supported=tuple(str(item) for item in supported),
                active=bool(row["active"]),
            )
        )
    return tuple(vehicles)