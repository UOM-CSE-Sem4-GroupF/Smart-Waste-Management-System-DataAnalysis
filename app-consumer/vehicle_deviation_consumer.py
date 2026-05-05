import os
import json
import logging
import socket
from dotenv import load_dotenv
from kafka import KafkaConsumer, TopicPartition

# Patch DNS resolution to handle Kafka's internal advertised listener
orig_getaddrinfo = socket.getaddrinfo
def patched_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    if host == 'controller.internal':
        host = os.getenv("KAFKA_BROKER", "163.47.8.3").split(':')[0]
    return orig_getaddrinfo(host, port, family, type, proto, flags)
socket.getaddrinfo = patched_getaddrinfo

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("vehicle-deviation-consumer")
logging.getLogger("kafka").setLevel(logging.WARNING)

load_dotenv()

BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
USER = os.getenv("KAFKA_USER")
PASS = os.getenv("KAFKA_PASS")
TOPIC = "waste.vehicle.deviation"

def run_consumer():
    logger.info(f"🔍 Monitoring VEHICLE DEVIATION topic: {TOPIC} on {BROKER}...")

    try:
        consumer = KafkaConsumer(
            bootstrap_servers=[BROKER],
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="SCRAM-SHA-256",
            sasl_plain_username=USER,
            sasl_plain_password=PASS,
            group_id=None,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            api_version=(2, 5, 0),
            request_timeout_ms=30000,
            fetch_max_wait_ms=500,
        )

        # Assuming 2 partitions for waste.vehicle.deviation
        partitions = [TopicPartition(TOPIC, p) for p in range(2)]
        consumer.assign(partitions)
        consumer.seek_to_end(*partitions)
        logger.info(f"✅ Assigned all 2 partitions, seeking to end (LIVE mode)...")

        while True:
            records = consumer.poll(timeout_ms=3000)
            if not records:
                continue

            for tp, messages in records.items():
                for message in messages:
                    payload = message.value
                    logger.info("🚨 RECEIVED VEHICLE DEVIATION EVENT:")
                    print(json.dumps(payload, indent=2), flush=True)
                    
                    inner = payload.get("payload", {})
                    vehicle_id = inner.get("vehicle_id")
                    deviation_m = inner.get("deviation_metres")
                    duration_s = inner.get("duration_seconds")
                    
                    logger.info(f"Result: Vehicle={vehicle_id}, Deviation={deviation_m}m, Duration={duration_s}s")
                    print("-" * 50, flush=True)

    except Exception as e:
        logger.error(f"❌ Kafka Error: {e}")

if __name__ == "__main__":
    run_consumer()
