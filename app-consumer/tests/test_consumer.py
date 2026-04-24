import os
import json
import uuid
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
USER = os.getenv("KAFKA_USER")
PASS = os.getenv("KAFKA_PASS")
TOPIC = os.getenv("KAFKA_TOPIC", "waste.bin.telemetry")
GROUP = f"test-{uuid.uuid4().hex[:6]}"

print(f"Connecting to {BROKER} as group '{GROUP}'...")
print(f"Topic: {TOPIC}")

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[BROKER],
    security_protocol="SASL_PLAINTEXT",
    sasl_mechanism="SCRAM-SHA-256",
    sasl_plain_username=USER,
    sasl_plain_password=PASS,
    auto_offset_reset='earliest',
    group_id=GROUP,
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    api_version=(2, 5, 0),
    consumer_timeout_ms=30000,  # Stop after 30s of no messages
)

print("Connected! Waiting for messages (30s timeout)...")

count = 0
for message in consumer:
    count += 1
    print(f"\n--- Message #{count} ---")
    print(f"Partition: {message.partition}, Offset: {message.offset}")
    print(json.dumps(message.value, indent=2))
    if count >= 5:
        print("\nGot 5 messages. Pipeline is WORKING!")
        break

if count == 0:
    print("No messages received after 30 seconds.")

consumer.close()
