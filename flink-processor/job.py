import argparse
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List

from kafka import KafkaConsumer

from config import load_settings
from metadata_store import MetadataStore
from processors.bin_telemetry import BinTelemetryProcessor
from sinks.influx_sink import InfluxSink
from sinks.kafka_sink import KafkaSink
from sinks.postgres_sink import PostgresSink


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Flink Pipeline 1 - Bin Telemetry Processor")
    parser.add_argument(
        "--mode",
        choices=["kafka", "local", "pyflink-local", "pyflink-kafka"],
        default="kafka",
        help="Execution mode. 'pyflink-kafka' runs the stateful KeyedProcessFunction pipeline.",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=0,
        help="Optional message limit (0 = unlimited). Useful for smoke runs.",
    )
    return parser


def process_single_event(
    raw_event: Dict[str, Any],
    processor: BinTelemetryProcessor,
    influx_sink: InfluxSink,
    postgres_sink: PostgresSink,
    kafka_sink: KafkaSink,
    logger: logging.Logger,
) -> bool:
    """Process one event and fan out to all sinks. Returns True if event was processed."""
    try:
        influx_sink.write_raw_event(raw_event)
        processed = processor.process(raw_event)
        if processed is None:
            return False

        influx_sink.write_processed_event(processed)
        postgres_sink.upsert_bin_current_state(processed)
        kafka_sink.publish_processed(processed)
        return True
    except Exception as exc:  # Keep pipeline resilient per event.
        logger.exception("Failed processing event: %s", exc)
        return False


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


def _load_local_event_json(file_path: str, logger: logging.Logger, max_messages: int) -> List[str]:
    events_json: List[str] = []
    for event in _read_local_events(file_path, logger):
        events_json.append(json.dumps(event, default=str))
        if max_messages > 0 and len(events_json) >= max_messages:
            break
    return events_json


def run_local_mode(
    settings,
    processor: BinTelemetryProcessor,
    influx_sink: InfluxSink,
    postgres_sink: PostgresSink,
    kafka_sink: KafkaSink,
    logger: logging.Logger,
    max_messages: int,
) -> None:
    processed_count = 0
    read_count = 0

    for event in _read_local_events(settings.local_test_input_file, logger):
        read_count += 1
        processed_ok = process_single_event(
            raw_event=event,
            processor=processor,
            influx_sink=influx_sink,
            postgres_sink=postgres_sink,
            kafka_sink=kafka_sink,
            logger=logger,
        )
        if processed_ok:
            processed_count += 1

        if max_messages > 0 and read_count >= max_messages:
            break

    logger.info(
        "Local mode completed: read=%d, processed=%d, dropped=%d",
        read_count,
        processed_count,
        read_count - processed_count,
    )


def _build_kafka_consumer(settings) -> KafkaConsumer:
    consumer_config = {
        "bootstrap_servers": settings.kafka_bootstrap_servers,
        "value_deserializer": lambda b: json.loads(b.decode("utf-8")),
        "auto_offset_reset": "latest",
        "api_version": (2, 5, 0),
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
    return KafkaConsumer(**consumer_config)


def run_kafka_mode(
    settings,
    processor: BinTelemetryProcessor,
    influx_sink: InfluxSink,
    postgres_sink: PostgresSink,
    kafka_sink: KafkaSink,
    logger: logging.Logger,
    max_messages: int,
) -> None:
    from kafka import TopicPartition
    consumer = _build_kafka_consumer(settings)
    
    # Manually assign partitions to bypass Group Coordinator hangs on remote clusters
    partitions = [TopicPartition(settings.kafka_input_topic, p) for p in range(6)]
    consumer.assign(partitions)
    consumer.seek_to_end(*partitions)
    
    processed_count = 0
    read_count = 0
    logger.info("Kafka mode started (Manual Assign). Consuming from topic: %s", settings.kafka_input_topic)

    try:
        for message in consumer:
            payload = message.value
            read_count += 1
            logger.info("Received raw message: %s", str(payload)[:100])

            if isinstance(payload, dict):
                processed_ok = process_single_event(
                    raw_event=payload,
                    processor=processor,
                    influx_sink=influx_sink,
                    postgres_sink=postgres_sink,
                    kafka_sink=kafka_sink,
                    logger=logger,
                )
                if processed_ok:
                    processed_count += 1
            else:
                logger.warning("Skipping kafka message with non-dict payload")

            if max_messages > 0 and read_count >= max_messages:
                logger.info("Reached max-messages=%d. Stopping consumption.", max_messages)
                break
    finally:
        consumer.close()

    logger.info(
        "Kafka mode completed: read=%d, processed=%d, dropped=%d",
        read_count,
        processed_count,
        read_count - processed_count,
    )


def run_pyflink_local_mode(settings, logger: logging.Logger, max_messages: int) -> None:
    """Run bounded local replay through PyFlink DataStream API."""
    try:
        from pyflink.common import Types
        from pyflink.datastream import StreamExecutionEnvironment
        from pyflink.datastream.functions import MapFunction
    except ImportError as exc:
        raise RuntimeError(
            "PyFlink is not available. Install requirements and retry pyflink-local mode."
        ) from exc

    events_json = _load_local_event_json(settings.local_test_input_file, logger, max_messages)
    if not events_json:
        logger.info("PyFlink local mode completed: read=0, processed=0, dropped=0")
        return

    class PipelineFanoutMap(MapFunction):
        def __init__(self, runtime_settings):
            self._settings = runtime_settings
            self._processor = None
            self._influx_sink = None
            self._postgres_sink = None
            self._kafka_sink = None
            self._metadata_store = None
            self._logger = logging.getLogger("flink-pipeline-1")

        def open(self, runtime_context):
            _ = runtime_context
            self._metadata_store = MetadataStore(self._settings)
            self._processor = BinTelemetryProcessor(self._metadata_store)
            self._influx_sink = InfluxSink(self._settings)
            self._postgres_sink = PostgresSink(self._settings)
            self._kafka_sink = KafkaSink(self._settings)

        def map(self, raw_event_json: str) -> bool:
            raw_event = json.loads(raw_event_json)
            return process_single_event(
                raw_event=raw_event,
                processor=self._processor,
                influx_sink=self._influx_sink,
                postgres_sink=self._postgres_sink,
                kafka_sink=self._kafka_sink,
                logger=self._logger,
            )

        def close(self):
            if self._kafka_sink is not None:
                self._kafka_sink.close()
            if self._postgres_sink is not None:
                self._postgres_sink.close()
            if self._influx_sink is not None:
                self._influx_sink.close()
            if self._metadata_store is not None:
                self._metadata_store.close()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    input_stream = env.from_collection(events_json, type_info=Types.STRING())
    processed_stream = input_stream.map(PipelineFanoutMap(settings), output_type=Types.BOOLEAN())

    processed_count = 0
    for processed_ok in processed_stream.execute_and_collect(
        job_execution_name="pipeline1-pyflink-local"
    ):
        if processed_ok:
            processed_count += 1

    total = len(events_json)
    logger.info(
        "PyFlink local mode completed: read=%d, processed=%d, dropped=%d",
        total,
        processed_count,
        total - processed_count,
    )


def run_pyflink_kafka_mode(settings, logger: logging.Logger) -> None:
    """
    Stateful PyFlink DataStream pipeline (Pipeline 1 — Bin Telemetry).

    Requires the PyFlink Kafka connector JAR on the classpath. Set the
    FLINK_KAFKA_JAR env var to the absolute path of the JAR, or place it at
    the default path printed in the error message when the JAR is missing.
    """
    try:
        from pyflink.common import Types, WatermarkStrategy
        from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
        from pyflink.datastream.connectors.kafka import (
            KafkaSource,
            KafkaOffsetsInitializer,
        )
        from pyflink.common.serialization import SimpleStringSchema
    except ImportError as exc:
        raise RuntimeError(
            "PyFlink is not installed. Run: pip install apache-flink==1.20.0"
        ) from exc

    from processors.bin_telemetry_flink import BinTelemetryFlinkProcessor
    from sinks.flink_sinks import (
        ProcessedBinInfluxFlinkSink,
        BinStateFlinkSink,
        KafkaProcessedFlinkSink,
    )

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)

    # Exactly-once checkpointing every 30 seconds (spec §3)
    env.enable_checkpointing(30_000)
    env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(5_000)

    # Kafka connector JAR — required for from_source()
    jar_path = os.getenv("FLINK_KAFKA_JAR", "")
    if jar_path and Path(jar_path).exists():
        env.add_jars(f"file://{Path(jar_path).resolve()}")
    elif jar_path:
        logger.warning("FLINK_KAFKA_JAR path does not exist: %s — proceeding without it", jar_path)

    # Build Kafka source
    source_builder = (
        KafkaSource.builder()
        .set_bootstrap_servers(settings.kafka_bootstrap_servers)
        .set_topics(settings.kafka_input_topic)
        .set_group_id("flink-pipeline1-pyflink")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
    )
    if settings.kafka_username and settings.kafka_password:
        source_builder = source_builder.set_properties(
            {
                "security.protocol": settings.kafka_security_protocol,
                "sasl.mechanism": settings.kafka_sasl_mechanism,
                "sasl.jaas.config": (
                    f"org.apache.kafka.common.security.scram.ScramLoginModule required "
                    f'username="{settings.kafka_username}" '
                    f'password="{settings.kafka_password}";'
                ),
            }
        )
    kafka_source = source_builder.build()

    raw_stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "Kafka:waste.bin.telemetry",
    )

    # Key by bin_id then run stateful processor; output is JSON strings
    processor = BinTelemetryFlinkProcessor()
    processed_stream = (
        raw_stream
        .key_by(
            lambda s: json.loads(s).get("payload", {}).get("bin_id", "unknown"),
            key_type=Types.STRING(),
        )
        .process(processor, output_type=Types.STRING())
    )

    # Chain sink MapFunctions. add_sink() requires Java interop; .map() works for pure-Python I/O.
    # Each step writes to its destination as a side effect and passes the JSON string through.
    # .print() at the end anchors the graph so Flink includes all operators in the execution plan.
    (
        processed_stream
        .map(ProcessedBinInfluxFlinkSink(settings), output_type=Types.STRING())
        .map(BinStateFlinkSink(settings), output_type=Types.STRING())
        .map(KafkaProcessedFlinkSink(settings), output_type=Types.STRING())
        .print()
    )

    logger.info("Submitting PyFlink job: Pipeline 1 — Bin Telemetry")
    env.execute("Pipeline 1 — Bin Telemetry (stateful)")


def main() -> None:
    args = build_arg_parser().parse_args()
    settings = load_settings()

    logging.basicConfig(level=settings.log_level)
    logger = logging.getLogger("flink-pipeline-1")

    logger.info("Pipeline execution started. Mode=%s", args.mode)
    metadata_store = None
    influx_sink = None
    postgres_sink = None
    kafka_sink = None

    try:
        if args.mode == "local":
            metadata_store = MetadataStore(settings)
            processor = BinTelemetryProcessor(metadata_store)
            influx_sink = InfluxSink(settings)
            postgres_sink = PostgresSink(settings)
            kafka_sink = KafkaSink(settings)
            run_local_mode(
                settings=settings,
                processor=processor,
                influx_sink=influx_sink,
                postgres_sink=postgres_sink,
                kafka_sink=kafka_sink,
                logger=logger,
                max_messages=args.max_messages,
            )
        elif args.mode == "pyflink-local":
            run_pyflink_local_mode(
                settings=settings,
                logger=logger,
                max_messages=args.max_messages,
            )
        elif args.mode == "pyflink-kafka":
            run_pyflink_kafka_mode(settings=settings, logger=logger)
        else:
            metadata_store = MetadataStore(settings)
            processor = BinTelemetryProcessor(metadata_store)
            influx_sink = InfluxSink(settings)
            postgres_sink = PostgresSink(settings)
            kafka_sink = KafkaSink(settings)
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
        if kafka_sink is not None:
            kafka_sink.close()
        if postgres_sink is not None:
            postgres_sink.close()
        if influx_sink is not None:
            influx_sink.close()
        if metadata_store is not None:
            metadata_store.close()
        logger.info("Pipeline shutdown complete")


if __name__ == "__main__":
    main()
