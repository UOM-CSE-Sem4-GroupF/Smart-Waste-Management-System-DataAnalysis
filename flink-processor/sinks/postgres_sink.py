import logging
from datetime import datetime
from typing import Any, Dict, Optional

import psycopg2
from psycopg2 import pool, sql

from config import Settings


logger = logging.getLogger(__name__)


class PostgresSinkError(Exception):
    pass


class PostgresSink:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._pool: Optional[pool.SimpleConnectionPool] = pool.SimpleConnectionPool(
            minconn=1,
            maxconn=5,
            host=self.settings.postgres_host,
            port=self.settings.postgres_port,
            database=self.settings.postgres_db,
            user=self.settings.postgres_user,
            password=self.settings.postgres_password,
        )

    @staticmethod
    def _to_datetime(value: Any) -> Optional[datetime]:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                return None
        return None

    def upsert_bin_current_state(self, record: Dict[str, Any]) -> None:
        # Column mapping aligned with database-schema-v3 :: f2.bin_current_state.
        # Denormalized fields (cluster_id, zone_id, waste_category_id, volume_litres)
        # are safe to store here — they never change after installation.
        conn = self._pool.getconn()
        try:
            with conn.cursor() as cur:
                query = sql.SQL(
                    """
                    INSERT INTO f2.bin_current_state (
                        bin_id,
                        fill_level_pct,
                        battery_level_pct,
                        estimated_weight_kg,
                        status,
                        urgency_score,
                        predicted_full_at,
                        fill_rate_pct_per_hour,
                        cluster_id,
                        zone_id,
                        waste_category_id,
                        volume_litres,
                        last_reading_at,
                        updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (bin_id) DO UPDATE SET
                        fill_level_pct          = EXCLUDED.fill_level_pct,
                        battery_level_pct       = EXCLUDED.battery_level_pct,
                        estimated_weight_kg     = EXCLUDED.estimated_weight_kg,
                        status                  = EXCLUDED.status,
                        urgency_score           = EXCLUDED.urgency_score,
                        predicted_full_at       = EXCLUDED.predicted_full_at,
                        fill_rate_pct_per_hour  = EXCLUDED.fill_rate_pct_per_hour,
                        cluster_id              = EXCLUDED.cluster_id,
                        zone_id                 = EXCLUDED.zone_id,
                        waste_category_id       = EXCLUDED.waste_category_id,
                        volume_litres           = EXCLUDED.volume_litres,
                        last_reading_at         = EXCLUDED.last_reading_at,
                        updated_at              = NOW()
                    """
                )
                cur.execute(
                    query,
                    (
                        record["bin_id"],
                        record["fill_level_pct"],
                        record.get("battery_level_pct"),
                        record.get("estimated_weight_kg"),
                        record["status"],
                        record.get("urgency_score"),
                        self._to_datetime(record.get("predicted_full_at")),
                        record.get("fill_rate_pct_per_hour"),
                        record.get("cluster_id"),
                        record.get("zone_id"),
                        record.get("waste_category_id"),
                        record.get("volume_litres"),
                        self._to_datetime(record.get("event_ts") or record.get("last_reading_at")),
                    ),
                )
            conn.commit()
        except (psycopg2.Error, KeyError) as exc:
            conn.rollback()
            raise PostgresSinkError(f"Failed to upsert bin_current_state: {exc}") from exc
        finally:
            self._pool.putconn(conn)

    def insert_zone_snapshot(self, record: Dict[str, Any]) -> None:
        conn = self._pool.getconn()
        try:
            with conn.cursor() as cur:
                query = sql.SQL(
                    """
                    INSERT INTO zone_snapshots (
                        zone_id,
                        snapshot_at,
                        avg_fill_level,
                        urgent_bin_count,
                        total_bins,
                        dominant_waste_category,
                        total_estimated_kg,
                        window_minutes
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """
                )
                cur.execute(
                    query,
                    (
                        record["zone_id"],
                        self._to_datetime(record.get("snapshot_at")),
                        record.get("avg_fill_level"),
                        record.get("urgent_bin_count"),
                        record.get("total_bins"),
                        record.get("dominant_waste_category"),
                        record.get("total_estimated_kg"),
                        record.get("window_minutes"),
                    ),
                )
            conn.commit()
        except (psycopg2.Error, KeyError) as exc:
            conn.rollback()
            raise PostgresSinkError(f"Failed to insert zone_snapshots: {exc}") from exc
        finally:
            self._pool.putconn(conn)

    def mark_bin_offline(self, bin_id: str) -> None:
        """
        Updates f2.bin_current_state to status='offline' and urgency_score=0.
        Called by the sensor offline detector when a bin stops reporting.
        Spec: 06-flink-processor.md §8 — SensorOfflineDetector.on_timer
        """
        conn = self._pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE f2.bin_current_state
                       SET status        = 'offline',
                           urgency_score = 0,
                           updated_at    = NOW()
                     WHERE bin_id = %s
                    """,
                    (bin_id,),
                )
            conn.commit()
            logger.info("Marked bin %s as offline in bin_current_state", bin_id)
        except psycopg2.Error as exc:
            conn.rollback()
            raise PostgresSinkError(f"Failed to mark bin {bin_id} offline: {exc}") from exc
        finally:
            self._pool.putconn(conn)

    def close(self) -> None:
        if self._pool is not None:
            self._pool.closeall()
            logger.debug("Closed PostgreSQL sink connection pool")
