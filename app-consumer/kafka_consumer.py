import os
import json
import logging
from dotenv import load_dotenv
from kafka import KafkaConsumer, TopicPartition

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("swms-consumer")
logging.getLogger("kafka").setLevel(logging.WARNING)

load_dotenv()

BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
USER = os.getenv("KAFKA_USER")
PASS = os.getenv("KAFKA_PASS")
TOPIC = os.getenv("KAFKA_TOPIC", "waste.bin.telemetry")

def run_consumer():
    logger.info(f"🚀 Starting SWMS Application Consumer...")
    logger.info(f"Connecting to Kafka at {BROKER}...")

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
        partitions = [TopicPartition(TOPIC, p) for p in range(6)]
        consumer.assign(partitions)
        consumer.seek_to_end(*partitions)

        logger.info(f"✅ Assigned all 6 partitions, seeking to end...")

        while True:
            records = consumer.poll(timeout_ms=3000)
            if not records:
                logger.info("poll() returned empty — no new messages yet")
                continue
            for tp, messages in records.items():
                for message in messages:
                    payload = message.value
                    logger.info(f"--- Message on partition {tp.partition} offset {message.offset} ---")
                    logger.info(f"Key: {message.key.decode('utf-8') if message.key else 'None'}")
                    print(json.dumps(payload, indent=2), flush=True)
                    inner = payload.get("payload", {})
                    fill_level = inner.get("fill_level_pct", 0)
                    bin_id = inner.get("bin_id", message.key.decode('utf-8') if message.key else "unknown")
                    if fill_level > 80:
                        logger.warning(f"ALERT: Bin {bin_id} is {fill_level}% full!")

    except Exception as e:
        logger.error(f"❌ Kafka Error: {e}")
        logger.info("TIP: Check your Kafka credentials and ensure the ELB endpoint is reachable.")

if __name__ == "__main__":
    run_consumer()
