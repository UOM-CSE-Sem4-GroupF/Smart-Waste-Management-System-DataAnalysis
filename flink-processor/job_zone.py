import argparse
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, Iterable

from kafka import KafkaConsumer

from config import load_settings
from processors.zone_aggregation import ValidationError, ZoneAggregationProcessor
from sinks.influx_sink import InfluxSink
from sinks.kafka_sink import KafkaSink
from sinks.postgres_sink import PostgresSink


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Flink Pipeline 2 - Zone Aggregation")
    parser.add_argument(
        "--mode",
        choices=["kafka", "local"],
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


def _read_local_events(file_path: str, logger: logging.Logger) -> Iterable[Dict[str, Any]]:
    input_file = Path(file_path)
    if not input_file.exists():
        raise FileNotFoundError(f"Local input file not found: {input_file}")

    with input_file.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            raw_line = line.strip()
            if not raw_line:
                continue
            try:
                event = json.loads(raw_line)
            except json.JSONDecodeError as exc:
                logger.warning("Skipping invalid JSON at line %d: %s", line_number, exc)
                continue

            if not isinstance(event, dict):
                logger.warning("Skipping non-object JSON at line %d", line_number)
                continue
            yield event


def _get_int_env(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value is None or raw_value.strip() == "":
        return default
    try:
        return int(raw_value)
    except ValueError:
        return default


def _build_kafka_consumer(settings) -> KafkaConsumer:
    consumer_config = {
        "bootstrap_servers": settings.kafka_bootstrap_servers,
        "value_deserializer": lambda b: json.loads(b.decode("utf-8")),
        "auto_offset_reset": "latest",
        "enable_auto_commit": True,
        "group_id": "flink-pipeline2-zone-aggregation",
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
    return KafkaConsumer(settings.kafka_zone_input_topic, **consumer_config)


def process_single_event(
    raw_event: Dict[str, Any],
    processor: ZoneAggregationProcessor,
    influx_sink: InfluxSink,
    postgres_sink: PostgresSink,
    kafka_sink: KafkaSink,
    logger: logging.Logger,
) -> int:
    try:
        snapshots = processor.process(raw_event)
        for snapshot in snapshots:
            postgres_sink.insert_zone_snapshot(snapshot)
            kafka_sink.publish_zone_statistics(snapshot)
            influx_sink.write_zone_statistics(snapshot)
        return len(snapshots)
    except ValidationError as exc:
        logger.warning("Skipping invalid processed-bin event: %s", exc)
        return 0
    except Exception as exc:
        logger.exception("Failed processing zone aggregation event: %s", exc)
        return 0


def run_local_mode(
    settings,
    processor: ZoneAggregationProcessor,
    influx_sink: InfluxSink,
    postgres_sink: PostgresSink,
    kafka_sink: KafkaSink,
    logger: logging.Logger,
    max_messages: int,
) -> None:
    read_count = 0
    emitted_count = 0

    for event in _read_local_events(settings.local_zone_test_input_file, logger):
        read_count += 1
        emitted_count += process_single_event(
            raw_event=event,
            processor=processor,
            influx_sink=influx_sink,
            postgres_sink=postgres_sink,
            kafka_sink=kafka_sink,
            logger=logger,
        )

        if max_messages > 0 and read_count >= max_messages:
            break

    logger.info(
        "Pipeline 2 local completed: read=%d emitted_snapshots=%d",
        read_count,
        emitted_count,
    )


def run_kafka_mode(
    settings,
    processor: ZoneAggregationProcessor,
    influx_sink: InfluxSink,
    postgres_sink: PostgresSink,
    kafka_sink: KafkaSink,
    logger: logging.Logger,
    max_messages: int,
) -> None:
    consumer = _build_kafka_consumer(settings)
    read_count = 0
    emitted_count = 0

    logger.info(
        "Pipeline 2 kafka mode started. Consuming from topic: %s",
        settings.kafka_zone_input_topic,
    )

    try:
        for message in consumer:
            payload = message.value
            read_count += 1

            if isinstance(payload, dict):
                emitted_count += process_single_event(
                    raw_event=payload,
                    processor=processor,
                    influx_sink=influx_sink,
                    postgres_sink=postgres_sink,
                    kafka_sink=kafka_sink,
                    logger=logger,
                )
            else:
                logger.warning("Skipping kafka message with non-dict payload")

            if max_messages > 0 and read_count >= max_messages:
                logger.info("Reached max-messages=%d. Stopping consumption.", max_messages)
                break
    finally:
        consumer.close()

    logger.info(
        "Pipeline 2 kafka completed: read=%d emitted_snapshots=%d",
        read_count,
        emitted_count,
    )


def main() -> None:
    args = build_arg_parser().parse_args()
    settings = load_settings()

    logging.basicConfig(level=settings.log_level)
    logger = logging.getLogger("flink-pipeline-2")

    window_minutes = _get_int_env("ZONE_WINDOW_MINUTES", 10)
    slide_minutes = _get_int_env("ZONE_SLIDE_MINUTES", 2)
    processor = ZoneAggregationProcessor(window_minutes=window_minutes, slide_minutes=slide_minutes)
    influx_sink = InfluxSink(settings)
    postgres_sink = PostgresSink(settings)
    kafka_sink = KafkaSink(settings)

    logger.info(
        "Pipeline 2 window config: window_minutes=%d slide_minutes=%d",
        window_minutes,
        slide_minutes,
    )

    try:
        if args.mode == "local":
            run_local_mode(
                settings=settings,
                processor=processor,
                influx_sink=influx_sink,
                postgres_sink=postgres_sink,
                kafka_sink=kafka_sink,
                logger=logger,
                max_messages=args.max_messages,
            )
        else:
            run_kafka_mode(
                settings=settings,
                processor=processor,
                influx_sink=influx_sink,
                postgres_sink=postgres_sink,
                kafka_sink=kafka_sink,
                logger=logger,
                max_messages=args.max_messages,
            )
    finally:
        kafka_sink.close()
        postgres_sink.close()
        influx_sink.close()
        logger.info("Pipeline 2 shutdown complete")


if __name__ == "__main__":
    main()