import json
import logging
from typing import Any, Dict

from kafka import KafkaProducer

from config import Settings


logger = logging.getLogger(__name__)


class KafkaSink:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        producer_config = {
            "bootstrap_servers": self.settings.kafka_bootstrap_servers,
            "value_serializer": lambda v: json.dumps(v, default=str).encode("utf-8"),
        }
        if self.settings.kafka_username and self.settings.kafka_password:
            producer_config.update(
                {
                    "security_protocol": self.settings.kafka_security_protocol,
                    "sasl_mechanism": self.settings.kafka_sasl_mechanism,
                    "sasl_plain_username": self.settings.kafka_username,
                    "sasl_plain_password": self.settings.kafka_password,
                }
            )
        self._producer = KafkaProducer(**producer_config)

    def publish_processed(self, event: Dict[str, Any]) -> None:
        future = self._producer.send(self.settings.kafka_output_topic, event)
        result = future.get(timeout=10)
        logger.debug(
            "Published processed event to Kafka topic=%s partition=%s offset=%s",
            self.settings.kafka_output_topic,
            result.partition,
            result.offset,
        )

    def publish_zone_statistics(self, event: Dict[str, Any]) -> None:
        future = self._producer.send(self.settings.kafka_zone_output_topic, event)
        result = future.get(timeout=10)
        logger.debug(
            "Published zone statistics to Kafka topic=%s partition=%s offset=%s",
            self.settings.kafka_zone_output_topic,
            result.partition,
            result.offset,
        )

    def publish_vehicle_deviation(self, event: Dict[str, Any]) -> None:
        future = self._producer.send(self.settings.kafka_vehicle_deviation_topic, event)
        result = future.get(timeout=10)
        logger.debug(
            "Published vehicle deviation to Kafka topic=%s partition=%s offset=%s",
            self.settings.kafka_vehicle_deviation_topic,
            result.partition,
            result.offset,
        )

    def close(self) -> None:
        self._producer.flush()
        self._producer.close()
