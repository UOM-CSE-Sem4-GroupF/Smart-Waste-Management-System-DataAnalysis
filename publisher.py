import paho.mqtt.client as mqtt
import time
import json
import random
from config import MQTT_HOST, MQTT_PORT

def create_mqtt_client():
    if hasattr(mqtt, "CallbackAPIVersion"):
        return mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION1)
    return mqtt.Client()


client = create_mqtt_client()

while True:
    try:
        client.connect(MQTT_HOST, MQTT_PORT, 60)
        break
    except Exception as e:
        print(f"MQTT not ready ({e}), retrying in 3s...")
        time.sleep(3)

i = 0
bins = ["bin1","bin2","bin3","bin4","bin5"]
while True:
    for bin_id in bins:
        data = {
            "bin_id": bin_id,
            "fill": random.randint(10, 100),
            "location": f"zone - {bin_id[-1]}",
            "timestamp": time.time()
        }
        topic = f"waste/{bin_id}"
        client.publish(topic, json.dumps(data))
        print("Data sent",topic, data)
        time.sleep(2)