"""
Pipeline 5 — Sensor Offline Detector job
==========================================
Spec reference: 06-flink-processor.md §8

Architecture
------------
Two concurrent activities run on a single thread via a polling loop:

1. Kafka Consumer  — reads from waste.bin.telemetry and calls
   detector.record_heartbeat() for every message received.

2. Periodic tick   — every TICK_INTERVAL_SECONDS the detector.tick()
   method is called.  Any bin that has been silent ≥ 30 minutes is
   declared offline; its DB row is updated and a Kafka alert published.

This avoids threads/async while keeping the semantics of the spec's
processing-time timer (check silences on a wall-clock interval).

Environment variables
---------------------
OFFLINE_THRESHOLD_MINUTES   Silence duration before marking a bin offline (default 30)
TICK_INTERVAL_SECONDS       How often the polling loop fires the timer check (default 60)
"""

import argparse
import json
import logging
import os
import time
from typing import Optional

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

from config import load_settings
from processors.sensor_offline import SensorOfflineDetector
from sinks.kafka_sink import KafkaSink
from sinks.postgres_sink import PostgresSink, PostgresSinkError


# ── Helpers ────────────────────────────────────────────────────────────────────

def _get_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Flink Pipeline 5 — Sensor Offline Detector"
    )
    parser.add_argument(
        "--mode",
        choices=["kafka"],
        default="kafka",
        help="Execution mode.",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=0,
        help="Stop after N messages (0 = unlimited). Useful for smoke runs.",
    )
    return parser


def _build_kafka_consumer(settings) -> KafkaConsumer:
    consumer_config = {
        "bootstrap_servers": settings.kafka_bootstrap_servers,
        "value_deserializer": lambda b: json.loads(b.decode("utf-8")),
        "auto_offset_reset": "latest",
        "enable_auto_commit": True,
        "group_id": "flink-pipeline5-sensor-offline-detector",
        # Short poll timeout so the loop can fire the tick check regularly
        # even when no messages are arriving.
        "consumer_timeout_ms": 5_000,
    }
    if settings.kafka_username and settings.kafka_password:
        consumer_config.update(
            {
                "security_protocol": settings.kafka_security_protocol,
                "sasl_mechanism": settings.kafka_sasl_mechanism,
                "sasl_plain_username": settings.kafka_username,
                "sasl_plain_password": settings.kafka_password,
            }
        )
    return KafkaConsumer(settings.kafka_input_topic, **consumer_config)


# ── Core processing ────────────────────────────────────────────────────────────

def _handle_offline_alert(
    alert: dict,
    postgres_sink: PostgresSink,
    kafka_sink: KafkaSink,
    logger: logging.Logger,
) -> None:
    """Writes the offline status to DB and publishes the alert to Kafka."""
    bin_id = alert["bin_id"]
    try:
        postgres_sink.mark_bin_offline(bin_id)
    except PostgresSinkError as exc:
        logger.error("Failed to mark bin %s offline in DB: %s", bin_id, exc)

    try:
        kafka_sink.publish_sensor_offline(alert)
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to publish sensor offline alert for bin %s: %s", bin_id, exc)


def run_kafka_mode(
    settings,
    detector: SensorOfflineDetector,
    postgres_sink: PostgresSink,
    kafka_sink: KafkaSink,
    logger: logging.Logger,
    max_messages: int,
    tick_interval_s: int,
) -> None:
    try:
        consumer = _build_kafka_consumer(settings)
    except NoBrokersAvailable as exc:
        logger.error(
            "Kafka broker unavailable at %s: %s",
            settings.kafka_bootstrap_servers,
            exc,
        )
        return

    read_count = 0
    alert_count = 0
    last_tick_at = time.monotonic()

    logger.info(
        "Pipeline 5 started. Consuming from topic=%s  threshold=%s min  tick=%s s",
        settings.kafka_input_topic,
        detector.offline_threshold,
        tick_interval_s,
    )

    try:
        # consumer_timeout_ms causes StopIteration when idle, so we loop manually
        while True:
            # ── Drain available messages ──────────────────────────────────────
            try:
                for message in consumer:
                    payload = message.value
                    if not isinstance(payload, dict):
                        logger.warning("Skipping non-dict Kafka message")
                        continue

                    bin_id: Optional[str] = payload.get("bin_id")
                    timestamp = payload.get("timestamp") or payload.get("event_ts")

                    if not bin_id or not timestamp:
                        logger.debug("Skipping event missing bin_id or timestamp")
                        continue

                    detector.record_heartbeat(bin_id, timestamp)
                    read_count += 1

                    if max_messages > 0 and read_count >= max_messages:
                        logger.info("Reached max-messages=%d. Stopping.", max_messages)
                        return
            except StopIteration:
                # consumer_timeout_ms expired — no messages right now
                pass

            # ── Periodic tick: check for silent bins ──────────────────────────
            now = time.monotonic()
            if now - last_tick_at >= tick_interval_s:
                last_tick_at = now
                alerts = detector.tick()
                for alert in alerts:
                    logger.warning(
                        "SENSOR_OFFLINE detected: bin=%s silence=%ss",
                        alert["bin_id"],
                        alert["silence_seconds"],
                    )
                    _handle_offline_alert(alert, postgres_sink, kafka_sink, logger)
                    alert_count += 1

    finally:
        consumer.close()
        logger.info(
            "Pipeline 5 shutdown: messages_read=%d offline_alerts=%d tracked_bins=%d",
            read_count,
            alert_count,
            len(detector.tracked_bins()),
        )


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    args = build_arg_parser().parse_args()
    settings = load_settings()

    logging.basicConfig(level=settings.log_level)
    logger = logging.getLogger("flink-pipeline-5")

    offline_threshold_minutes = _get_int_env("OFFLINE_THRESHOLD_MINUTES", 30)
    tick_interval_s = _get_int_env("TICK_INTERVAL_SECONDS", 60)

    logger.info(
        "Pipeline 5 config: offline_threshold=%d min  tick_interval=%d s",
        offline_threshold_minutes,
        tick_interval_s,
    )

    detector = SensorOfflineDetector(offline_threshold_minutes=offline_threshold_minutes)
    postgres_sink = PostgresSink(settings)
    kafka_sink = KafkaSink(settings)

    try:
        run_kafka_mode(
            settings=settings,
            detector=detector,
            postgres_sink=postgres_sink,
            kafka_sink=kafka_sink,
            logger=logger,
            max_messages=args.max_messages,
            tick_interval_s=tick_interval_s,
        )
    finally:
        kafka_sink.close()
        postgres_sink.close()
        logger.info("Pipeline 5 shutdown complete")


if __name__ == "__main__":
    main()
