import argparse
import json
import logging
import sys
import uuid
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import psycopg2
from influxdb_client import InfluxDBClient
from kafka import KafkaConsumer


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import load_settings  # noqa: E402


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("e2e-verify-outputs")


def _is_disabled_text(value: Optional[str]) -> bool:
    if value is None:
        return True
    return value.strip().lower() in {"", "none", "disabled", "false", "off"}


def _kafka_consumer(topic: str, settings, timeout_s: int) -> KafkaConsumer:
    config = {
        "bootstrap_servers": settings.kafka_bootstrap_servers,
        "value_deserializer": lambda b: json.loads(b.decode("utf-8")),
        "auto_offset_reset": "earliest",
        "enable_auto_commit": False,
        "group_id": f"e2e-verify-{topic}-{uuid.uuid4().hex[:8]}",
        "consumer_timeout_ms": timeout_s * 1000,
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
    return KafkaConsumer(topic, **config)


def verify_kafka_topic(topic: str, settings, timeout_s: int, max_messages: int) -> Tuple[str, str]:
    if _is_disabled_text(settings.kafka_bootstrap_servers) or _is_disabled_text(topic):
        return ("skipped", f"Kafka verification skipped for topic={topic}: Kafka disabled in .env")

    consumer = _kafka_consumer(topic, settings, timeout_s)
    count = 0
    sample: Optional[Dict[str, Any]] = None
    try:
        for message in consumer:
            count += 1
            if sample is None and isinstance(message.value, dict):
                sample = message.value
            if count >= max_messages:
                break
    finally:
        consumer.close()

    if count == 0:
        return ("ok", f"Kafka topic={topic}: no messages observed in verification window")

    sample_text = ""
    if sample is not None:
        sample_text = f" sample_keys={sorted(sample.keys())}"
    return ("ok", f"Kafka topic={topic}: observed_messages={count}.{sample_text}")


def verify_postgres(settings) -> Tuple[str, str]:
    if _is_disabled_text(settings.postgres_host):
        return ("skipped", "PostgreSQL verification skipped: PostgreSQL disabled in .env")

    conn = psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        database=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM bin_current_state")
            bin_state_count = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM zone_snapshots")
            zone_snapshot_count = cur.fetchone()[0]

        return (
            "ok",
            (
                "PostgreSQL records: "
                f"bin_current_state={bin_state_count}, zone_snapshots={zone_snapshot_count}"
            ),
        )
    finally:
        conn.close()


def _query_measurement_count(client: InfluxDBClient, org: str, bucket: str, measurement: str) -> int:
    flux = f'''
from(bucket: "{bucket}")
  |> range(start: -24h)
  |> filter(fn: (r) => r._measurement == "{measurement}")
  |> count(column: "_value")
'''
    tables = client.query_api().query(query=flux, org=org)

    total = 0
    for table in tables:
        for record in table.records:
            value = record.get_value()
            if isinstance(value, int):
                total += value
    return total


def verify_influx(settings) -> Tuple[str, str]:
    if not settings.influx_enabled:
        return ("skipped", "InfluxDB verification skipped: INFLUX_ENABLED=false")
    if _is_disabled_text(settings.influx_url):
        return ("skipped", "InfluxDB verification skipped: INFLUX_URL disabled in .env")

    checks = [
        (settings.influx_raw_bucket, "bin_readings_raw"),
        (settings.influx_processed_bucket, "bin_readings_processed"),
        (settings.influx_zone_bucket, "zone_statistics"),
        (settings.influx_vehicle_bucket, "vehicle_positions"),
    ]

    with InfluxDBClient(url=settings.influx_url, token=settings.influx_token, org=settings.influx_org) as client:
        parts = []
        for bucket, measurement in checks:
            count = _query_measurement_count(client, settings.influx_org, bucket, measurement)
            parts.append(f"{measurement}@{bucket}={count}")

    return ("ok", "InfluxDB writes (last 24h): " + ", ".join(parts))


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify E2E outputs across Kafka, PostgreSQL, and InfluxDB")
    parser.add_argument("--timeout-seconds", type=int, default=4, help="Kafka read timeout per topic")
    parser.add_argument("--max-messages", type=int, default=20, help="Max messages read per topic")
    args = parser.parse_args()

    settings = load_settings()

    kafka_topics = [
        settings.kafka_output_topic,
        settings.kafka_zone_output_topic,
        settings.kafka_vehicle_deviation_topic,
    ]

    logger.info("Starting E2E output verification")

    for topic in kafka_topics:
        try:
            status, text = verify_kafka_topic(topic, settings, args.timeout_seconds, args.max_messages)
            log_fn = logger.info if status in {"ok", "skipped"} else logger.error
            log_fn(text)
        except Exception as exc:
            logger.error("Kafka verification failed for topic=%s: %s", topic, exc)

    try:
        status, text = verify_postgres(settings)
        log_fn = logger.info if status in {"ok", "skipped"} else logger.error
        log_fn(text)
    except Exception as exc:
        logger.error("PostgreSQL verification failed: %s", exc)

    try:
        status, text = verify_influx(settings)
        log_fn = logger.info if status in {"ok", "skipped"} else logger.error
        log_fn(text)
    except Exception as exc:
        logger.error("InfluxDB verification failed: %s", exc)

    logger.info("E2E output verification finished")


if __name__ == "__main__":
    main()
