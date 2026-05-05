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
logger = logging.getLogger("zone-statistics-consumer")
logging.getLogger("kafka").setLevel(logging.WARNING)

load_dotenv()

BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
USER = os.getenv("KAFKA_USER")
PASS = os.getenv("KAFKA_PASS")
TOPIC = "waste.zone.statistics"

def run_consumer():
    logger.info(f"🔍 Monitoring ZONE STATISTICS topic: {TOPIC} on {BROKER}...")

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

        partitions = [TopicPartition(TOPIC, p) for p in range(3)]
        consumer.assign(partitions)
        consumer.seek_to_end(*partitions)
        logger.info(f"✅ Assigned all 3 partitions, seeking to end (LIVE mode)...")

        while True:
            records = consumer.poll(timeout_ms=3000)
            if not records:
                continue

            for tp, messages in records.items():
                for message in messages:
                    payload = message.value
                    logger.info("✅ RECEIVED ZONE STATISTICS EVENT:")
                    print(json.dumps(payload, indent=2), flush=True)
                    
                    inner = payload.get("payload", {})
                    zone_id = inner.get("zone_id")
                    avg_fill = inner.get("avg_fill_level_pct")
                    urgent = inner.get("urgent_bin_count")
                    critical = inner.get("critical_bin_count")
                    
                    logger.info(f"Result: Zone={zone_id}, AvgFill={avg_fill}%, Urgent={urgent}, Critical={critical}")
                    print("-" * 50, flush=True)

    except Exception as e:
        logger.error(f"❌ Kafka Error: {e}")

if __name__ == "__main__":
    run_consumer()
