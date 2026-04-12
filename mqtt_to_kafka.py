import json
import paho.mqtt.client as mqtt
from kafka import KafkaProducer
from config import MQTT_HOST, MQTT_PORT, KAFKA_BOOTSTRAP_SERVERS

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

KAFKA_TOPIC = 'waste-stream'

def on_message(client, userdata, msg):
    data = json.loads(msg.payload.decode())
    print("MQTT Recieved:", data)

    producer.send(KAFKA_TOPIC, data)
    print("Sent to kafka:", data)

mqtt_client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION1)
mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)

mqtt_client.subscribe("waste/#")

mqtt_client.on_message = on_message

print("MQTT to Kafka bridge is running...")
mqtt_client.loop_forever()