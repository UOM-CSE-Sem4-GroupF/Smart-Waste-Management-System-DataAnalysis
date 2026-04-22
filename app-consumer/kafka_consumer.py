import os
import json
import logging
from dotenv import load_dotenv
from kafka import KafkaConsumer, TopicPartition

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("swms-consumer")

# Suppress kafka-python connection noise. The KRaft controller (node 0) is advertised
# in Kafka cluster metadata but only resolves inside the cluster. External clients see
# DNS failures for controller.internal — these are harmless, the broker (node 100)
# handles all client traffic. Suppressing here keeps output readable.
logging.getLogger("kafka").setLevel(logging.WARNING)
logging.getLogger("kafka.conn").setLevel(logging.CRITICAL)

load_dotenv()

BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
USER = os.getenv("KAFKA_USER")
PASS = os.getenv("KAFKA_PASS")
TOPIC = os.getenv("KAFKA_TOPIC", "waste.bin.telemetry")

def run_consumer():
    logger.info(f"🚀 Starting SWMS Application Consumer...")
    logger.info(f"Connecting to Kafka at {BROKER}...")

    # When running EXTERNALLY (NLB endpoint), use direct partition assignment.
    # Consumer groups require FindCoordinator which loops through all cluster nodes
    # including the KRaft controller (controller.internal) — not resolvable outside
    # the cluster and blocks the IO loop indefinitely.
    # When running INSIDE the cluster (pod), controller.internal resolves fine so
    # group_id='swms-app-group' can be restored for proper offset tracking.
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
        )
        tp = TopicPartition(TOPIC, 0)
        consumer.assign([tp])
        consumer.seek_to_end(tp)
        
        logger.info(f"✅ Successfully subscribed to topic: {TOPIC}")
        logger.info("Waiting for messages... (Run the simulator to see data flow)")

        for message in consumer:
            payload = message.value
            logger.info("--- New Message Received ---")
            logger.info(f"Topic: {message.topic}")
            logger.info(f"Key: {message.key.decode('utf-8') if message.key else 'None'}")
            
            # Print the data in a pretty format
            print(json.dumps(payload, indent=2), flush=True)
            
            # Trigger alert if bin is nearly full
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
