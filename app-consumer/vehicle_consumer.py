import os
import json
import logging
from dotenv import load_dotenv
from kafka import KafkaConsumer, TopicPartition

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("swms-vehicle-consumer")
logging.getLogger("kafka").setLevel(logging.WARNING)

load_dotenv()

# Configuration from Environment
BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
USER = os.getenv("KAFKA_USER")
PASS = os.getenv("KAFKA_PASS")
# Use the vehicle topic from .env
TOPIC = os.getenv("KAFKA_TOPIC_VEHICLE", "waste.vehicle.location")

def run_consumer():
    logger.info(f"🚀 Starting SWMS Vehicle Location Consumer...")
    logger.info(f"Connecting to Kafka at {BROKER} for topic {TOPIC}...")

    try:
        # Following the pattern from working kafka_consumer.py
        consumer = KafkaConsumer(
            bootstrap_servers=[BROKER],
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="SCRAM-SHA-256",
            sasl_plain_username=USER,
            sasl_plain_password=PASS,
            group_id=None, # Manual assignment pattern
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            api_version=(2, 5, 0),
            request_timeout_ms=30000,
            fetch_max_wait_ms=500,
        )

        # Manually assign partitions (assuming 6 partitions as per topics.yaml)
        partitions = [TopicPartition(TOPIC, p) for p in range(6)]
        consumer.assign(partitions)
        
        # Seek to the end to only see LIVE pings from the simulator
        consumer.seek_to_end(*partitions)

        logger.info(f"✅ Assigned all 6 partitions, seeking to end (LIVE mode)...")

        while True:
            # Use poll() pattern
            records = consumer.poll(timeout_ms=3000)
            if not records:
                # logger.info("poll() returned empty — no new vehicle pings yet")
                continue
                
            for tp, messages in records.items():
                for message in messages:
                    payload = message.value
                    key = message.key.decode('utf-8') if message.key else "None"
                    
                    logger.info(f"--- Vehicle {key} on partition {tp.partition} offset {message.offset} ---")
                    
                    # Extract raw data from bridge wrapper
                    data = payload.get("payload", {})
                    print(json.dumps(data, indent=2), flush=True)
                    
                    if "lat" in data and "lon" in data:
                        logger.info(f"   GPS: {data['lat']}, {data['lon']} | Speed: {data.get('speed', 0)} km/h")
                    
                    print("-" * 50, flush=True)

    except Exception as e:
        logger.error(f"❌ Kafka Error: {e}")
        logger.info("TIP: If you still see 'controller.internal' errors, ensure your VPN is active or the advertised.listeners are correctly set for external access.")

if __name__ == "__main__":
    run_consumer()
