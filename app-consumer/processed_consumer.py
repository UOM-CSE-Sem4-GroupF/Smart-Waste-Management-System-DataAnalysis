import os
import json
import logging
from dotenv import load_dotenv
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("processed-consumer")

load_dotenv()

BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = "waste.bin.processed"

def run_consumer():
    logger.info(f"🔍 Monitoring PROCESSED topic: {TOPIC}...")

    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=[BROKER],
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True
        )

        for message in consumer:
            payload = message.value
            logger.info("✅ RECEIVED PROCESSED EVENT:")
            print(json.dumps(payload, indent=2), flush=True)
            
            inner = payload.get("payload", {})
            bin_id = inner.get("bin_id")
            status = inner.get("status")
            score = inner.get("urgency_score")
            
            logger.info(f"Result: Bin={bin_id}, Status={status}, Score={score}")

    except Exception as e:
        logger.error(f"❌ Kafka Error: {e}")

if __name__ == "__main__":
    run_consumer()
