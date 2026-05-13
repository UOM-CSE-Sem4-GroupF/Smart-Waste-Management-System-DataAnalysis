import os
import json
import logging
from kafka import KafkaProducer

logger = logging.getLogger(__name__)

def publish_event(topic, event_name, models):
    """Publish a JSON event to Kafka."""
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    try:
        producer = KafkaProducer(
            bootstrap_servers=[bootstrap],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        message = {
            "event": event_name,
            "timestamp": __import__("datetime").datetime.now().isoformat(),
            "source": "airflow",
            "models": models
        }
        producer.send(topic, message)
        producer.flush()
        producer.close()
        logger.info(f"Published {event_name} to {topic}")
    except Exception as e:
        logger.error(f"Kafka publish failed: {e}")
