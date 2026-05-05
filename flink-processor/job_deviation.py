import argparse
import json
import logging
import os
import time

from kafka import KafkaConsumer, TopicPartition
from kafka.errors import NoBrokersAvailable

from config import load_settings
from processors.vehicle_deviation import ValidationError, VehicleDeviationProcessor
from route_store import RouteStore, RouteStoreError
from sinks.kafka_sink import KafkaSink


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Flink Pipeline 3 - Vehicle Deviation Detector")
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
        help="Optional message limit (0 = unlimited). Useful for smoke runs.",
    )
    return parser


def _get_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _build_kafka_consumer(settings) -> KafkaConsumer:
    """
    Matches the working pattern from app-consumer/vehicle_consumer.py:
    - group_id=None  (no consumer group — manual partition assignment)
    - assign() all partitions of the vehicle location topic
    - seek_to_end() so we only consume live GPS pings
    - poll() loop instead of blocking iterator

    waste.vehicle.location is configured with 4 partitions in docker-compose.yml.
    """
    num_partitions = _get_int_env("KAFKA_VEHICLE_LOCATION_PARTITIONS", 4)

    consumer_config = {
        "bootstrap_servers": settings.kafka_bootstrap_servers,
        "value_deserializer": lambda b: b.decode("utf-8"),
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
    partitions = [TopicPartition(settings.kafka_vehicle_location_topic, p) for p in range(num_partitions)]
    consumer.assign(partitions)
    consumer.seek_to_end(*partitions)
    return consumer


def process_single_event(
    raw_event,
    processor: VehicleDeviationProcessor,
    kafka_sink: KafkaSink,
    logger: logging.Logger,
) -> bool:
    try:
        alert = processor.process(raw_event)
        if alert is None:
            return False
        kafka_sink.publish_vehicle_deviation(alert)
        return True
    except ValidationError as exc:
        logger.warning("Skipping invalid vehicle location event: %s", exc)
        return False
    except Exception as exc:
        logger.exception("Failed processing vehicle deviation event: %s", exc)
        return False


def run_kafka_mode(
    settings,
    processor: VehicleDeviationProcessor,
    kafka_sink: KafkaSink,
    logger: logging.Logger,
    max_messages: int,
) -> None:
    try:
        consumer = _build_kafka_consumer(settings)
    except NoBrokersAvailable as exc:
        logger.error(
            "Kafka broker unavailable for topic %s at %s: %s",
            settings.kafka_vehicle_location_topic,
            settings.kafka_bootstrap_servers,
            exc,
        )
        return

    processed_count = 0
    read_count = 0

    logger.info(
        "Pipeline 3 kafka mode started. Consuming from topic=%s (group_id=None, manual assign)",
        settings.kafka_vehicle_location_topic,
    )

    last_status_at = time.monotonic()
    try:
        while True:
            records = consumer.poll(timeout_ms=3000)
            if not records:
                if time.monotonic() - last_status_at > 30:
                    logger.info("Pipeline 3 heartbeat: Waiting for new messages from %s...", settings.kafka_vehicle_location_topic)
                    last_status_at = time.monotonic()
                continue
            
            last_status_at = time.monotonic()

            for tp, messages in records.items():
                for message in messages:
                    payload = message.value
                    read_count += 1
                    logger.info("Received vehicle location: %s", str(payload)[:100])
                    if process_single_event(payload, processor, kafka_sink, logger):
                        processed_count += 1

                    if max_messages > 0 and read_count >= max_messages:
                        logger.info("Reached max-messages=%d. Stopping.", max_messages)
                        return
    finally:
        consumer.close()

    logger.info(
        "Pipeline 3 kafka completed: read=%d alerts=%d",
        read_count,
        processed_count,
    )


def main() -> None:
    args = build_arg_parser().parse_args()
    settings = load_settings()

    logging.basicConfig(level=settings.log_level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("flink-pipeline-3")

    # Suppress noisy library logs
    logging.getLogger("kafka").setLevel(logging.ERROR)

    route_store = None
    kafka_sink = None

    try:
        route_store = RouteStore(settings)
        processor = VehicleDeviationProcessor(route_store)
        kafka_sink = KafkaSink(settings)
        run_kafka_mode(
            settings=settings,
            processor=processor,
            kafka_sink=kafka_sink,
            logger=logger,
            max_messages=args.max_messages,
        )
    except RouteStoreError as exc:
        logger.error("Failed to initialize route store: %s", exc)
    finally:
        if kafka_sink is not None:
            kafka_sink.close()
        if route_store is not None:
            route_store.close()
        logger.info("Pipeline 3 shutdown complete")


if __name__ == "__main__":
    main()