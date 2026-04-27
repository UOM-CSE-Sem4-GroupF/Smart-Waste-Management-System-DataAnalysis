import argparse
import logging

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

from config import load_settings
from processors.vehicle_position import ValidationError, VehiclePositionProcessor
from sinks.influx_sink import InfluxSink


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Flink Pipeline 4 - Vehicle Position Historian")
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


def _build_kafka_consumer(settings) -> KafkaConsumer:
    consumer_config = {
        "bootstrap_servers": settings.kafka_bootstrap_servers,
        "value_deserializer": lambda b: b.decode("utf-8"),
        "auto_offset_reset": "latest",
        "enable_auto_commit": True,
        "group_id": "flink-pipeline4-vehicle-position-historian",
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
    return KafkaConsumer(settings.kafka_vehicle_location_topic, **consumer_config)


def process_single_event(
    raw_event,
    processor: VehiclePositionProcessor,
    influx_sink: InfluxSink,
    logger: logging.Logger,
) -> bool:
    try:
        event = processor.process(raw_event)
        influx_sink.write_vehicle_position(event)
        return True
    except ValidationError as exc:
        logger.warning("Skipping invalid vehicle location event: %s", exc)
        return False
    except Exception as exc:
        logger.exception("Failed processing vehicle location event: %s", exc)
        return False


def run_kafka_mode(
    settings,
    processor: VehiclePositionProcessor,
    influx_sink: InfluxSink,
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
        "Pipeline 4 kafka mode started. Consuming from topic: %s",
        settings.kafka_vehicle_location_topic,
    )

    try:
        for message in consumer:
            payload = message.value
            read_count += 1

            if process_single_event(
                raw_event=payload,
                processor=processor,
                influx_sink=influx_sink,
                logger=logger,
            ):
                processed_count += 1

            if max_messages > 0 and read_count >= max_messages:
                logger.info("Reached max-messages=%d. Stopping consumption.", max_messages)
                break
    finally:
        consumer.close()

    logger.info(
        "Pipeline 4 kafka completed: read=%d processed=%d dropped=%d",
        read_count,
        processed_count,
        read_count - processed_count,
    )


def main() -> None:
    args = build_arg_parser().parse_args()
    settings = load_settings()

    logging.basicConfig(level=settings.log_level)
    logger = logging.getLogger("flink-pipeline-4")

    processor = VehiclePositionProcessor()
    influx_sink = InfluxSink(settings)

    try:
        run_kafka_mode(
            settings=settings,
            processor=processor,
            influx_sink=influx_sink,
            logger=logger,
            max_messages=args.max_messages,
        )
    finally:
        influx_sink.close()
        logger.info("Pipeline 4 shutdown complete")


if __name__ == "__main__":
    main()