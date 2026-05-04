import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

import psycopg2
from influxdb_client import InfluxDBClient
from kafka import KafkaConsumer

try:
    from ...config import load_settings
except ImportError:
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))
    from config import load_settings


def _consumer_for_topic(settings, topic: str, group_id: Optional[str] = None, start_from_latest: bool = True) -> KafkaConsumer:
    broker = os.getenv("KAFKA_BROKER") or settings.kafka_bootstrap_servers
    username = os.getenv("KAFKA_USER") or settings.kafka_username
    password = os.getenv("KAFKA_PASS") or settings.kafka_password
    kwargs = {
        "bootstrap_servers": broker.split(","),
        "auto_offset_reset": "latest" if start_from_latest else "earliest",
        "enable_auto_commit": False,
        "consumer_timeout_ms": 1000,
        "value_deserializer": lambda v: json.loads(v.decode("utf-8")) if v is not None else None,
    }
    if group_id:
        kwargs["group_id"] = group_id
    if username and password:
        kwargs.update(
            {
                "security_protocol": settings.kafka_security_protocol or "SASL_PLAINTEXT",
                "sasl_mechanism": settings.kafka_sasl_mechanism or "SCRAM-SHA-256",
                "sasl_plain_username": username,
                "sasl_plain_password": password,
            }
        )

    return KafkaConsumer(topic, **kwargs)


def create_kafka_watcher(settings, topic: str, *, start_from_latest: bool = True) -> KafkaConsumer:
    group_id = f"e2e-watch-{topic}-{int(time.time() * 1000)}"
    return _consumer_for_topic(settings, topic, group_id=group_id, start_from_latest=start_from_latest)


def wait_for_kafka_match(
    consumer: KafkaConsumer,
    matcher: Callable[[Any], bool],
    timeout_s: int = 60,
) -> Tuple[bool, str, Optional[Any]]:
    deadline = time.monotonic() + timeout_s
    observed = 0
    last_error: Optional[str] = None

    while time.monotonic() < deadline:
        try:
            messages = consumer.poll(timeout_ms=1000, max_records=50)
            for records in messages.values():
                for record in records:
                    observed += 1
                    payload = record.value
                    if matcher(payload):
                        return True, f"observed matching Kafka message after {observed} records", payload
        except Exception as exc:
            last_error = str(exc)
            time.sleep(1)
            continue

    if last_error:
        return False, f"Kafka poll failed after {observed} records: {last_error}", None
    return False, f"no matching Kafka message observed after {observed} records in {timeout_s}s", None


def check_kafka_topic_for_test_run(settings, topic: str, test_run_id: str, timeout_s: int = 60) -> bool:
    consumer = create_kafka_watcher(settings, topic, start_from_latest=True)
    try:
        found, _, _ = wait_for_kafka_match(
            consumer,
            lambda payload: isinstance(payload, dict)
            and isinstance(payload.get("payload"), dict)
            and payload["payload"].get("test_run_id") == test_run_id,
            timeout_s=timeout_s,
        )
        return found
    finally:
        try:
            consumer.close()
        except Exception:
            pass


def _connect_postgres(settings):
    return psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        database=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
    )


def _table_exists(conn, table_name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (table_name,))
        row = cur.fetchone()
        return bool(row and row[0])


def _fetch_rows(conn, query: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(query, params)
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]


def get_bins_for_zone(settings, min_bins: int = 4) -> Optional[Dict[str, Any]]:
    conn = _connect_postgres(settings)
    try:
        rows = _fetch_rows(
            conn,
            """
            SELECT
                b.id AS bin_id,
                b.zone_id,
                b.waste_category_id,
                COALESCE(wc.name, '') AS waste_category,
                b.volume_litres,
                b.lat,
                b.lng
            FROM f2.bins b
            LEFT JOIN f2.waste_categories wc ON wc.id = b.waste_category_id
            WHERE b.active = TRUE
            ORDER BY b.zone_id, b.id
            """,
        )
    finally:
        conn.close()

    grouped: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[int(row["zone_id"])].append(row)

    best_zone: Optional[Dict[str, Any]] = None
    for zone_id, items in sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0])):
        if len(items) < min_bins:
            continue
        category_ids = {item["waste_category_id"] for item in items if item.get("waste_category_id") is not None}
        candidate = {
            "zone_id": zone_id,
            "bins": items[:min_bins],
            "distinct_category_count": len(category_ids),
        }
        best_zone = candidate
        if len(category_ids) >= 2:
            return candidate

    return best_zone


def get_route_plan_seed(settings) -> Optional[Dict[str, Any]]:
    conn = _connect_postgres(settings)
    try:
        candidate_tables = ["f2.route_plans", "route_plans"]
        for table_name in candidate_tables:
            if not _table_exists(conn, table_name):
                continue
            try:
                rows = _fetch_rows(
                    conn,
                    f"""
                    SELECT vehicle_id, job_id, waypoints
                    FROM {table_name}
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                )
            except Exception:
                continue
            if not rows:
                continue
            row = rows[0]
            waypoints = row.get("waypoints")
            if isinstance(waypoints, str):
                try:
                    waypoints = json.loads(waypoints)
                except Exception:
                    waypoints = []
            if isinstance(waypoints, dict):
                for key in ("waypoints", "route", "points", "stops"):
                    if isinstance(waypoints.get(key), list):
                        waypoints = waypoints[key]
                        break
            if not isinstance(waypoints, list):
                waypoints = []
            normalized_waypoints = []
            for waypoint in waypoints:
                if not isinstance(waypoint, dict):
                    continue
                latitude = waypoint.get("latitude", waypoint.get("lat"))
                longitude = waypoint.get("longitude", waypoint.get("lng", waypoint.get("lon")))
                if isinstance(latitude, (int, float)) and isinstance(longitude, (int, float)):
                    normalized_waypoints.append({"latitude": float(latitude), "longitude": float(longitude)})

            if not normalized_waypoints:
                continue

            return {
                "vehicle_id": str(row.get("vehicle_id")),
                "job_id": str(row.get("job_id")) if row.get("job_id") is not None else None,
                "waypoints": normalized_waypoints,
                "source_table": table_name,
            }

        return None
    finally:
        conn.close()


def check_postgres_bin_state_updated(settings, bin_ids: Sequence[str], since: datetime) -> Tuple[bool, str]:
    conn = _connect_postgres(settings)
    try:
        missing: List[str] = []
        with conn.cursor() as cur:
            for bin_id in bin_ids:
                cur.execute(
                    "SELECT updated_at FROM f2.bin_current_state WHERE bin_id = %s",
                    (bin_id,),
                )
                row = cur.fetchone()
                if not row or row[0] < since:
                    missing.append(bin_id)
        if missing:
            return False, f"bin_current_state not updated for bins={missing} since={since.isoformat()}"
        return True, f"bin_current_state updated for {len(bin_ids)} bins since={since.isoformat()}"
    finally:
        conn.close()


def check_postgres_zone_snapshot_since(settings, zone_id: int, since: datetime) -> Tuple[bool, str]:
    conn = _connect_postgres(settings)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT zone_id, snapshot_at FROM f2.zone_snapshots WHERE zone_id = %s AND snapshot_at >= %s LIMIT 1",
                (zone_id, since),
            )
            row = cur.fetchone()
            if row:
                return True, f"zone_snapshots updated for zone_id={zone_id} since={since.isoformat()}"
            return False, f"no zone_snapshots row for zone_id={zone_id} since={since.isoformat()}"
    finally:
        conn.close()


def check_influx_for_measurement(
    settings,
    bucket: str,
    measurement: str,
    tag_key: Optional[str] = None,
    tag_value: Optional[str] = None,
    timeout_s: int = 60,
    field_key: Optional[str] = None,
    field_value: Optional[str] = None,
    range_hours: int = 24,
) -> Tuple[bool, str]:
    if not settings.influx_enabled:
        return False, "InfluxDB verification skipped: INFLUX_ENABLED=false"

    deadline = time.monotonic() + timeout_s
    query = (
        f'from(bucket:"{bucket}") '
        f'|> range(start: -{range_hours}h) '
        f'|> filter(fn: (r) => r._measurement == "{measurement}")'
    )

    with InfluxDBClient(url=settings.influx_url, token=settings.influx_token, org=settings.influx_org) as client:
        query_api = client.query_api()
        last_error: Optional[str] = None
        observed = 0

        while time.monotonic() < deadline:
            try:
                tables = query_api.query(query=query, org=settings.influx_org)
                for table in tables:
                    for record in table.records:
                        observed += 1
                        values = record.values
                        if tag_key is not None and tag_value is not None:
                            if str(values.get(tag_key)) != str(tag_value):
                                continue
                        if field_key is not None and field_value is not None:
                            if record.get_field() != field_key:
                                continue
                            if str(record.get_value()) != str(field_value):
                                continue
                        return True, (
                            f"Influx measurement={measurement}@{bucket} matched after {observed} records"
                        )
            except Exception as exc:
                last_error = str(exc)
            time.sleep(1)

        if last_error:
            return False, f"Influx query failed for {measurement}@{bucket}: {last_error}"
        return False, f"no matching record found for {measurement}@{bucket} after {observed} records"


def check_kafka_alert_for_deviation(
    consumer: KafkaConsumer,
    *,
    vehicle_id: str,
    job_id: Optional[str],
    timeout_s: int = 60,
) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
    def matcher(payload: Any) -> bool:
        if not isinstance(payload, dict):
            return False
        if payload.get("vehicle_id") != vehicle_id:
            return False
        if job_id is not None and payload.get("job_id") != job_id:
            return False
        deviation_m = payload.get("deviation_m")
        duration_s = payload.get("duration_s")
        return isinstance(deviation_m, (int, float)) and deviation_m > 500 and isinstance(duration_s, (int, float)) and duration_s > 120

    return wait_for_kafka_match(consumer, matcher, timeout_s=timeout_s)


def check_kafka_zone_snapshot(
    consumer: KafkaConsumer,
    *,
    zone_id: int,
    timeout_s: int = 60,
) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
    def matcher(payload: Any) -> bool:
        if not isinstance(payload, dict):
            return False
        if int(payload.get("zone_id", -1)) != zone_id:
            return False
        snapshot_at = payload.get("snapshot_at")
        window_minutes = payload.get("window_minutes")
        return isinstance(snapshot_at, str) and isinstance(window_minutes, int)

    return wait_for_kafka_match(consumer, matcher, timeout_s=timeout_s)


def check_kafka_bin_processed(
    consumer: KafkaConsumer,
    *,
    bin_ids: Sequence[str],
    timeout_s: int = 60,
) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
    target_ids = {str(bin_id) for bin_id in bin_ids}

    def matcher(payload: Any) -> bool:
        if not isinstance(payload, dict):
            return False
        if payload.get("bin_id") not in target_ids:
            return False
        return isinstance(payload.get("event_ts"), str) and isinstance(payload.get("status"), str)

    return wait_for_kafka_match(consumer, matcher, timeout_s=timeout_s)
