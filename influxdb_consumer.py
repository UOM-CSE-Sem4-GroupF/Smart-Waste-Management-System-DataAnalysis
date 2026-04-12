from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import json
from config import KAFKA_BOOTSTRAP_SERVERS, INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_ORG, INFLUXDB_BUCKET

client = InfluxDBClient(
    url=INFLUXDB_URL,
    token=INFLUXDB_TOKEN,
    org=INFLUXDB_ORG
)

write_api = client.write_api(write_options=SYNCHRONOUS)

consumer = KafkaConsumer(
    'waste-stream',
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    group_id='influxdb-consumer-group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
)

for msg in consumer:
    data = msg.value

    point = Point("waste") \
        .tag("bin_id", str(data["bin_id"])) \
        .field("fill", data["fill"]) \
        .time(int(data["timestamp"] * 1e9))

    try:
        write_api.write(bucket=INFLUXDB_BUCKET, record=point)
        print("Data written to InfluxDB:", data)
    except Exception as e:
        print(f"Failed to write to InfluxDB: {e}")