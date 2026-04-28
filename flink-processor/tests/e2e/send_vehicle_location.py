import argparse
import json
import logging
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import psycopg2
from kafka import KafkaProducer


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import load_settings  # noqa: E402


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("e2e-send-vehicle-location")


def _build_producer(settings) -> KafkaProducer:
    config = {
        "bootstrap_servers": settings.kafka_bootstrap_servers,
        "value_serializer": lambda value: json.dumps(value, default=str).encode("utf-8"),
    }
    if settings.kafka_username and settings.kafka_password:
        config.update(
            {
                "security_protocol": settings.kafka_security_protocol,
                "sasl_mechanism": settings.kafka_sasl_mechanism,
                "sasl_plain_username": settings.kafka_username,
                "sasl_plain_password": settings.kafka_password,
            }
        )
    return KafkaProducer(**config)


def _gps(vehicle_id: str, job_id: Optional[str], ts: datetime, lat: float, lon: float) -> Dict[str, Any]:
    event: Dict[str, Any] = {
        "vehicle_id": vehicle_id,
        "latitude": lat,
        "longitude": lon,
        "timestamp": ts.isoformat().replace("+00:00", "Z"),
    }
    if job_id:
        event["job_id"] = job_id
    return event


def ensure_route_plan(settings, vehicle_id: str, job_id: Optional[str]) -> None:
    waypoints: List[Dict[str, float]] = [
        {"latitude": 6.9271, "longitude": 79.8612},
        {"latitude": 6.9280, "longitude": 79.8620},
        {"latitude": 6.9290, "longitude": 79.8630},
    ]

    conn = psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        database=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
    )
    try:
        with conn.cursor() as cur:
            if job_id:
                cur.execute(
                    """
                    SELECT id
                    FROM route_plans
                    WHERE vehicle_id = %s AND job_id = %s::uuid
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (vehicle_id, job_id),
                )
            else:
                cur.execute(
                    """
                    SELECT id
                    FROM route_plans
                    WHERE vehicle_id = %s
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (vehicle_id,),
                )

            existing = cur.fetchone()
            if existing is not None:
                logger.info("Route plan already exists for vehicle_id=%s", vehicle_id)
                return

            route_plan_id = str(uuid.uuid4())
            cur.execute(
                """
                INSERT INTO route_plans (
                    id,
                    job_id,
                    vehicle_id,
                    route_type,
                    zone_id,
                    waypoints,
                    total_bins,
                    estimated_weight_kg,
                    estimated_distance_km,
                    estimated_minutes,
                    valid_for_date,
                    status
                ) VALUES (
                    %s::uuid,
                    %s::uuid,
                    %s,
                    %s,
                    %s,
                    %s::jsonb,
                    %s,
                    %s,
                    %s,
                    %s,
                    CURRENT_DATE,
                    %s
                )
                """,
                (
                    route_plan_id,
                    job_id,
                    vehicle_id,
                    "routine",
                    1,
                    json.dumps(waypoints),
                    len(waypoints),
                    120.0,
                    4.5,
                    35,
                    "active",
                ),
            )
            conn.commit()
            logger.info("Inserted route plan id=%s for vehicle_id=%s", route_plan_id, vehicle_id)
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Send E2E vehicle GPS events")
    parser.add_argument("--vehicle-id", default="LORRY-01", help="Vehicle identifier")
    parser.add_argument(
        "--job-id",
        default="22222222-2222-2222-2222-222222222222",
        help="Route job UUID used by deviation detector",
    )
    parser.add_argument("--skip-route-plan-check", action="store_true", help="Do not seed/check route_plans")
    parser.add_argument("--sleep-seconds", type=float, default=0.2, help="Delay between sends")
    args = parser.parse_args()

    settings = load_settings()
    topic = settings.kafka_vehicle_location_topic

    if not args.skip_route_plan_check:
        ensure_route_plan(settings, args.vehicle_id, args.job_id)

    base = datetime.now(timezone.utc)
    events = [
        {
            "name": "normal route-following GPS point",
            "event": _gps(args.vehicle_id, args.job_id, base + timedelta(seconds=0), 6.9271, 79.8612),
        },
        {
            "name": "deviated GPS point",
            "event": _gps(args.vehicle_id, args.job_id, base + timedelta(seconds=30), 6.9900, 79.9500),
        },
        {
            "name": "deviation > 2 minutes",
            "event": _gps(args.vehicle_id, args.job_id, base + timedelta(seconds=151), 6.9905, 79.9505),
        },
    ]

    producer = _build_producer(settings)
    logger.info("Sending %d vehicle GPS scenarios to topic=%s", len(events), topic)

    try:
        for item in events:
            future = producer.send(topic, item["event"])
            result = future.get(timeout=10)
            payload = item["event"]
            logger.info(
                "Sent %-30s vehicle_id=%s lat=%.5f lon=%.5f ts=%s offset=%s",
                item["name"],
                payload["vehicle_id"],
                float(payload["latitude"]),
                float(payload["longitude"]),
                payload["timestamp"],
                result.offset,
            )
            if args.sleep_seconds > 0:
                time.sleep(args.sleep_seconds)
    finally:
        producer.flush()
        producer.close()

    logger.info("Completed sending vehicle location E2E events")


if __name__ == "__main__":
    main()
