import json
import time
import paho.mqtt.client as mqtt
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from config import MQTT_HOST, MQTT_PORT, KAFKA_BOOTSTRAP_SERVERS

def create_mqtt_client():
    if hasattr(mqtt, "CallbackAPIVersion"):
        return mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION1)
    return mqtt.Client()


while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        break
    except NoBrokersAvailable:
        print("Kafka not ready, retrying in 3s...")
        time.sleep(3)

KAFKA_TOPIC = 'waste-stream'

def on_message(client, userdata, msg):
    data = json.loads(msg.payload.decode())
    print("MQTT Recieved:", data)

    producer.send(KAFKA_TOPIC, data)
    print("Sent to kafka:", data)

mqtt_client = create_mqtt_client()

while True:
    try:
        mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)
        break
    except Exception as e:
        print(f"MQTT not ready ({e}), retrying in 3s...")
        time.sleep(3)

mqtt_client.subscribe("waste/#")

mqtt_client.on_message = on_message

print("MQTT to Kafka bridge is running...")
mqtt_client.loop_forever()