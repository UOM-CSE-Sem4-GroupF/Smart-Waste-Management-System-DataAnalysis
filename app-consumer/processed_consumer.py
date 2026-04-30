import os
import json
import logging
import socket
from dotenv import load_dotenv
from kafka import KafkaConsumer

# Patch DNS resolution to handle Kafka's internal advertised listener
orig_getaddrinfo = socket.getaddrinfo
def patched_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    if host == 'controller.internal':
        host = os.getenv("KAFKA_BROKER", "163.47.8.3").split(':')[0]
    return orig_getaddrinfo(host, port, family, type, proto, flags)
socket.getaddrinfo = patched_getaddrinfo

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("processed-consumer")

load_dotenv()

BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
USER = os.getenv("KAFKA_USER")
PASS = os.getenv("KAFKA_PASS")
TOPIC = "waste.bin.processed"

def run_consumer():
    logger.info(f"🔍 Monitoring PROCESSED topic: {TOPIC} on {BROKER}...")

    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=[BROKER],
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="SCRAM-SHA-256",
            sasl_plain_username=USER,
            sasl_plain_password=PASS,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True,
            api_version=(2, 5, 0)
        )

        for message in consumer:
            payload = message.value
            logger.info("✅ RECEIVED PROCESSED EVENT:")
            print(json.dumps(payload, indent=2), flush=True)
            
            # Pull fields from the root level (V3 format)
            bin_id = payload.get("bin_id")
            status = payload.get("status")
            score = payload.get("urgency_score")
            
            logger.info(f"Result: Bin={bin_id}, Status={status}, Score={score}")

    except Exception as e:
        logger.error(f"❌ Kafka Error: {e}")

if __name__ == "__main__":
    run_consumer()
