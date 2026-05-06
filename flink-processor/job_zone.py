import argparse
import json
import logging
import os
import time
from typing import Any, Dict, Iterable, List

from kafka import KafkaConsumer, TopicPartition

from config import load_settings
from processors.zone_aggregation import ValidationError, ZoneAggregationProcessor
from sinks.influx_sink import InfluxSink
from sinks.kafka_sink import KafkaSink
from sinks.postgres_sink import PostgresSink


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Flink Pipeline 2 - Zone Aggregation")
    parser.add_argument(
        "--mode",
        choices=["kafka", "local", "pyflink-kafka"],
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
    raw_value = os.getenv(name)
    if raw_value is None or raw_value.strip() == "":
        return default
    try:
        return int(raw_value)
    except ValueError:
        return default


def _read_local_events(file_path: str, logger: logging.Logger) -> Iterable[Dict[str, Any]]:
    from pathlib import Path
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


def _build_kafka_consumer(settings) -> KafkaConsumer:
    """
    Matches the working pattern from app-consumer/kafka_consumer.py:
    - group_id=None  (no consumer group — manual partition assignment)
    - assign() all partitions of the zone-input topic
    - seek_to_end() so we only consume live events
    - poll() loop instead of blocking iterator
    """
    # waste.bin.processed is configured with 6 partitions in docker-compose.yml
    num_partitions = _get_int_env("KAFKA_ZONE_INPUT_PARTITIONS", 6)

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
    partitions = [TopicPartition(settings.kafka_zone_input_topic, p) for p in range(num_partitions)]
    consumer.assign(partitions)
    consumer.seek_to_end(*partitions)
    return consumer


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
        "Pipeline 2 kafka mode started. Consuming from topic=%s (group_id=None, manual assign)",
        settings.kafka_zone_input_topic,
    )

    last_status_at = time.monotonic()
    try:
        while True:
            records = consumer.poll(timeout_ms=3000)
            if not records:
                if time.monotonic() - last_status_at > 30:
                    logger.info("Pipeline 2 heartbeat: Waiting for new messages from %s...", settings.kafka_zone_input_topic)
                    last_status_at = time.monotonic()
                continue
            
            last_status_at = time.monotonic()

            for tp, messages in records.items():
                for message in messages:
                    payload = message.value
                    if not isinstance(payload, dict):
                        logger.warning("Skipping non-dict message on partition %s", tp.partition)
                        continue

                    read_count += 1
                    logger.info("Received event for zone processing: %s", str(payload)[:100])
                    emitted_count += process_single_event(
                        raw_event=payload,
                        processor=processor,
                        influx_sink=influx_sink,
                        postgres_sink=postgres_sink,
                        kafka_sink=kafka_sink,
                        logger=logger,
                    )

                    if max_messages > 0 and read_count >= max_messages:
                        logger.info("Reached max-messages=%d. Stopping.", max_messages)
                        return
    finally:
        consumer.close()

    logger.info(
        "Pipeline 2 kafka completed: read=%d emitted_snapshots=%d",
        read_count,
        emitted_count,
    )


def run_pyflink_kafka_mode(settings) -> None:
    """
    Flink-native version of Pipeline 2.
    Uses Kafka Source (SQL Connector) + ZoneAggregationFlinkSink (FlatMap).
    """
    from pyflink.common import WatermarkStrategy
    from pyflink.common.typeinfo import Types
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
    from pyflink.datastream.formats.json import JsonRowDeserializationSchema

    from sinks.flink_sinks import ZoneAggregationFlinkSink

    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Optional: Load Kafka SQL JAR if needed (should be in /opt/flink/lib already)
    # env.add_jars(f"file://{settings.flink_kafka_jar}")

    # 1. Source: waste.bin.processed
    # We use SimpleStringSchema for maximum flexibility matching Pipeline 1.
    from pyflink.common.serialization import SimpleStringSchema
    source_str = (
        KafkaSource.builder()
        .set_bootstrap_servers(settings.kafka_bootstrap_servers)
        .set_topics(settings.kafka_zone_input_topic)
        .set_group_id("flink-pipeline-2-group")
        .set_value_only_deserializer(SimpleStringSchema())
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
    )

    if settings.kafka_username and settings.kafka_password:
        source_str.set_property("security.protocol", settings.kafka_security_protocol)
        source_str.set_property("sasl.mechanism", settings.kafka_sasl_mechanism)
        source_str.set_property("sasl.jaas.config", 
            f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{settings.kafka_username}" password="{settings.kafka_password}";')

    ds = env.from_source(source_str.build(), WatermarkStrategy.no_watermarks(), "KafkaProcessedBinSource")

    (
        ds
        .flat_map(ZoneAggregationFlinkSink(settings), output_type=Types.STRING())
        .print()
    )

    env.execute("Flink Pipeline 2 - Zone Aggregation")

def main() -> None:
    args = build_arg_parser().parse_args()
    settings = load_settings()

    logging.basicConfig(level=settings.log_level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("flink-pipeline-2")

    # Suppress noisy library logs
    logging.getLogger("kafka").setLevel(logging.ERROR)

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
        elif args.mode == "pyflink-kafka":
            run_pyflink_kafka_mode(settings)
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