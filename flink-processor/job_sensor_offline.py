"""
Pipeline 5 — Sensor Offline Detector job
==========================================
Spec reference: 06-flink-processor.md §8

Architecture
------------
Two activities run in a single poll() loop:

1. Kafka Consumer  — reads waste.bin.telemetry via manual partition
   assignment (group_id=None, seek_to_end) matching the pattern used
   in app-consumer/kafka_consumer.py.  Calls detector.record_heartbeat()
   for every message received.

2. Periodic tick   — every TICK_INTERVAL_SECONDS the detector.tick()
   method is called.  Any bin silent ≥ 30 minutes is declared offline;
   its DB row is updated and a Kafka alert published.

Environment variables
---------------------
KAFKA_TELEMETRY_PARTITIONS     Number of partitions on waste.bin.telemetry (default 6)
OFFLINE_THRESHOLD_MINUTES      Silence duration before marking a bin offline (default 30)
TICK_INTERVAL_SECONDS          How often the polling loop fires the timer check (default 60)
"""

import argparse
import json
import logging
import os
import time
from typing import Optional

from kafka import KafkaConsumer, TopicPartition
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
        choices=["kafka", "pyflink-kafka"],
        default="pyflink-kafka",
        help="Execution mode.",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=0,
        help="Stop after N messages (0 = unlimited). Useful for smoke runs.",
    )
    return parser


def run_pyflink_kafka_mode(settings, offline_threshold_min: int) -> None:
    """Sets up the stateful PyFlink pipeline for sensor offline detection."""
    from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
    from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
    from pyflink.common.serialization import SimpleStringSchema
    from pyflink.common.watermark_strategy import WatermarkStrategy
    
    from processors.sensor_offline_flink import SensorOfflineFlinkProcessor
    from sinks.flink_sinks import SensorOfflineFlinkSink

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    
    # Adding JARs for Kafka connector
    kafka_jar = os.getenv("FLINK_KAFKA_JAR")
    if kafka_jar and os.path.exists(kafka_jar):
        env.add_jars(f"file://{kafka_jar}")
    else:
        print(f"WARNING: Kafka JAR not found at {kafka_jar}. PyFlink might fail to connect to Kafka.")

    source = KafkaSource.builder() \
        .set_bootstrap_servers(settings.kafka_bootstrap_servers) \
        .set_topics(settings.kafka_input_topic) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_properties({
            "security.protocol": settings.kafka_security_protocol,
            "sasl.mechanism": settings.kafka_sasl_mechanism,
            "sasl.jaas.config": f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{settings.kafka_username}" password="{settings.kafka_password}";',
            "group.id": "flink-pipeline5-group"
        }) \
        .build()

    ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "TelemetrySource")

    def extract_bin_id(value):
        try:
            return json.loads(value).get("bin_id", "unknown")
        except:
            return "unknown"

    ds.key_by(extract_bin_id) \
      .process(SensorOfflineFlinkProcessor(offline_threshold_minutes=offline_threshold_min)) \
      .map(SensorOfflineFlinkSink(settings)) \
      .print()

    env.execute("SWMS Pipeline 5: Sensor Offline Detector")


def _build_kafka_consumer(settings) -> KafkaConsumer:
    """
    Matches the working pattern from app-consumer/kafka_consumer.py:
    - group_id=None  (no consumer group — manual partition assignment)
    - assign() all partitions of the telemetry topic
    - seek_to_end() so we only consume live events
    - poll() loop instead of blocking iterator

    waste.bin.telemetry is configured with 6 partitions in docker-compose.yml.
    """
    num_partitions = _get_int_env("KAFKA_TELEMETRY_PARTITIONS", 6)

    consumer_config = {
        "bootstrap_servers": settings.kafka_bootstrap_servers,
        "value_deserializer": lambda b: json.loads(b.decode("utf-8")),
        "group_id": None,
        "api_version": (2, 5, 0),
        "request_timeout_ms": 30000,
        "fetch_max_wait_ms": 500,
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

    consumer = KafkaConsumer(**consumer_config)
    partitions = [TopicPartition(settings.kafka_input_topic, p) for p in range(num_partitions)]
    consumer.assign(partitions)
    consumer.seek_to_end(*partitions)
    return consumer


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
        "Pipeline 5 started. topic=%s  threshold=%s  tick=%ss  (group_id=None, manual assign)",
        settings.kafka_input_topic,
        detector.offline_threshold,
        tick_interval_s,
    )

    last_status_at = time.monotonic()
    try:
        while True:
            # ── poll() for live messages ──────────────────────────────────────
            records = consumer.poll(timeout_ms=3000)

            if not records:
                if time.monotonic() - last_status_at > 30:
                    logger.info("Pipeline 5 heartbeat: Waiting for heartbeats from %s...", settings.kafka_input_topic)
                    last_status_at = time.monotonic()
            else:
                last_status_at = time.monotonic()

            for tp, messages in records.items():
                for message in messages:
                    payload = message.value
                    if not isinstance(payload, dict):
                        logger.warning("Skipping non-dict message on partition %s", tp.partition)
                        continue

                    bin_id: Optional[str] = payload.get("bin_id")
                    timestamp = payload.get("timestamp") or payload.get("event_ts")

                    if not bin_id or not timestamp:
                        logger.debug("Skipping event missing bin_id or timestamp")
                        continue

                    detector.record_heartbeat(bin_id, timestamp)
                    read_count += 1
                    logger.info("Received heartbeat for offline detection: bin_id=%s", bin_id)

                    if max_messages > 0 and read_count >= max_messages:
                        logger.info("Reached max-messages=%d. Stopping.", max_messages)
                        return

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

    logging.basicConfig(level=settings.log_level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("flink-pipeline-5")

    # Suppress noisy library logs
    logging.getLogger("kafka").setLevel(logging.ERROR)

    offline_threshold_minutes = _get_int_env("OFFLINE_THRESHOLD_MINUTES", 30)
    tick_interval_s = _get_int_env("TICK_INTERVAL_SECONDS", 60)

    logger.info(
        "Pipeline 5 starting in %s mode. threshold=%d min",
        args.mode,
        offline_threshold_minutes,
    )

    if args.mode == "pyflink-kafka":
        run_pyflink_kafka_mode(settings, offline_threshold_minutes)
        return

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
