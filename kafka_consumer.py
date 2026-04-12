from kafka import KafkaConsumer
import json
from config import KAFKA_BOOTSTRAP_SERVERS

print("Starting Kafka consumer...")

consumer = KafkaConsumer(
    'waste-stream',
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    group_id='kafka-consumer-group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

for msg in consumer:
    print("Kafka received:", msg.value)