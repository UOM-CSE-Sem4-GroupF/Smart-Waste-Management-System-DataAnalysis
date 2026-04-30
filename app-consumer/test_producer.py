import os
import json
import logging
import time
from dotenv import load_dotenv
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("test-producer")

load_dotenv()

BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
USER = os.getenv("KAFKA_USER")
PASS = os.getenv("KAFKA_PASS")
TOPIC = os.getenv("KAFKA_TOPIC", "waste.bin.telemetry")

def run_producer():
    logger.info(f"📤 Sending test event to topic: {TOPIC} on {BROKER}...")

    producer = KafkaProducer(
        bootstrap_servers=[BROKER],
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_plain_username=USER,
        sasl_plain_password=PASS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(2, 5, 0)
    )

    test_payload = {
        "version": "1.0-bridge",
        "source_service": "emqx-oss-bridge",
        "timestamp": int(time.time() * 1000),
        "payload": {
            "bin_id": "BIN-009",
            "fill_level_pct": 62.9,
            "battery_level_pct": 51.7,
            "signal_strength_dbm": -72,
            "temperature_c": 25.2,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "firmware_version": "2.1.4",
            "error_flags": 0
        }
    }

    try:
        future = producer.send(TOPIC, value=test_payload)
        metadata = future.get(timeout=10)
        logger.info(f"✅ Message sent to {metadata.topic} partition {metadata.partition} offset {metadata.offset}")
    except Exception as e:
        logger.error(f"❌ Failed to send message: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    run_producer()
