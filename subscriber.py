import paho.mqtt.client as mqtt
import json
from config import MQTT_HOST, MQTT_PORT

def on_message(Client, userdata, msg):
    data = json.loads(msg.payload.decode())
    print("Received :",data)

client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION1)
client.connect(MQTT_HOST, MQTT_PORT, 60)
client.subscribe("waste/#")

client.on_message = on_message
client.loop_forever()