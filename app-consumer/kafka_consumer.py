import os
import json
import logging
from dotenv import load_dotenv
from kafka import KafkaConsumer, TopicPartition
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("swms-consumer")
logging.getLogger("kafka").setLevel(logging.WARNING)

load_dotenv()

BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")  # Default to docker service name
USER = os.getenv("KAFKA_USER", "").strip()
PASS = os.getenv("KAFKA_PASS", "").strip()
TOPIC = os.getenv("KAFKA_TOPIC", "waste.bin.telemetry")
SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")

def run_consumer():
    logger.info(f"🚀 Starting SWMS Application Consumer...")
    logger.info(f"Connecting to Kafka at {BROKER}...")
    logger.info(f"Topic: {TOPIC}")
    logger.info(f"Security protocol: {SECURITY_PROTOCOL}")

    # Prepare consumer config
    consumer_config = {
        "bootstrap_servers": [BROKER],
        "value_deserializer": lambda v: json.loads(v.decode('utf-8')),
        "group_id": "test-consumer",
        "auto_offset_reset": "earliest",
        "enable_auto_commit": False,
        "request_timeout_ms": 30000,
        "fetch_max_wait_ms": 500,
        "api_version": (2, 5, 0),
    }

    # Add SASL auth only if credentials provided
    if USER and PASS:
        consumer_config["security_protocol"] = "SASL_PLAINTEXT"
        consumer_config["sasl_mechanism"] = "SCRAM-SHA-256"
        consumer_config["sasl_plain_username"] = USER
        consumer_config["sasl_plain_password"] = PASS
    elif SECURITY_PROTOCOL == "PLAINTEXT":
        consumer_config["security_protocol"] = "PLAINTEXT"

    max_retries = 5
    retry_count = 0

    while retry_count < max_retries:
        try:
            logger.info(f"Attempt {retry_count + 1}/{max_retries} to connect to Kafka...")
            consumer = KafkaConsumer(**consumer_config)
            logger.info("✅ Connected to Kafka successfully!")
            break
        except Exception as e:
            retry_count += 1
            logger.warning(f"Failed to connect to Kafka: {e}")
            if retry_count < max_retries:
                wait_time = 5 * retry_count
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logger.error("Max retries exceeded. Exiting.")
                raise

    try:
        # Try to get partition count
        try:
            partitions = consumer.partitions_for_topic(TOPIC)
            if partitions is None:
                logger.warning(f"Topic {TOPIC} not found, waiting for it...")
                time.sleep(5)
                partitions = consumer.partitions_for_topic(TOPIC)
            
            if partitions:
                partition_list = [TopicPartition(TOPIC, p) for p in partitions]
                consumer.assign(partition_list)
                consumer.seek_to_end(*partition_list)
                logger.info(f"✅ Assigned {len(partitions)} partitions, seeking to end...")
            else:
                logger.error(f"Topic {TOPIC} has no partitions")
                consumer.subscribe([TOPIC])
                logger.info(f"✅ Subscribed to {TOPIC}")
        except Exception as e:
            logger.warning(f"Could not get partitions: {e}, subscribing instead...")
            consumer.subscribe([TOPIC])

        message_count = 0
        while True:
            records = consumer.poll(timeout_ms=3000)
            if not records:
                logger.debug("Poll returned empty — no new messages")
                continue
            
            for tp, messages in records.items():
                for message in messages:
                    message_count += 1
                    payload = message.value
                    logger.info(f"[{message_count}] Partition {tp.partition}, Offset {message.offset}")
                    logger.info(f"  Key: {message.key.decode('utf-8') if message.key else 'None'}")
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
