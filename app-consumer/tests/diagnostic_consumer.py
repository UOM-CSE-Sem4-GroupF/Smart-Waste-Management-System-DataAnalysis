import os
import json
import logging
import uuid
from dotenv import load_dotenv
from kafka import KafkaConsumer

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("swms-diagnostic")

# Load environment variables
load_dotenv()

BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
USER = os.getenv("KAFKA_USER")
PASS = os.getenv("KAFKA_PASS")
TOPIC = os.getenv("KAFKA_TOPIC", "waste.bin.telemetry")

# CRITICAL: We use a random suffix for the group ID to force a fresh start
NEW_GROUP_ID = f"swms-diag-group-{uuid.uuid4().hex[:6]}"

def run_diagnostic():
    logger.info(f"🔍 Starting Diagnostic Consumer ({NEW_GROUP_ID})...")
    logger.info(f"Targeting Topic: {TOPIC}")
    logger.info(f"Brokers: {BROKER}")
    
    try:
        # Initialize the Consumer
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=[BROKER],
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="SCRAM-SHA-256",
            sasl_plain_username=USER,
            sasl_plain_password=PASS,
            auto_offset_reset='earliest', # Start from scratch
            group_id=NEW_GROUP_ID,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            # Add some debugging options
            api_version=(2, 5, 0),
            request_timeout_ms=30000,
            retry_backoff_ms=500
        )
        
        logger.info(f"✅ Connection handshake successful. Waiting for partition assignment...")
        
        # Check assigned partitions
        partitions = consumer.partitions_for_topic(TOPIC)
        logger.info(f"Topic '{TOPIC}' partitions: {partitions}")

        logger.info("⏱️ Listening for real-time messages... (Wait at least 20 seconds)")

        for message in consumer:
            payload = message.value
            logger.info("✨ MESSAGE RECEIVED!")
            print(json.dumps(payload, indent=2))
            
    except Exception as e:
        logger.error(f"❌ Diagnostic Failure: {e}")

if __name__ == "__main__":
    run_diagnostic()
