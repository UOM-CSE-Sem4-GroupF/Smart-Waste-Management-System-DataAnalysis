#!/usr/bin/env python3
"""Verify outputs from all 4 Flink pipelines"""
import sys
from pathlib import Path
from collections import OrderedDict
import json

sys.path.insert(0, str(Path(__file__).resolve().parent / "flink-processor"))

from config import load_settings
import psycopg2
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient

settings = load_settings()

print("=" * 80)
print("PIPELINE OUTPUT VERIFICATION REPORT")
print("=" * 80)

# ============ POSTGRES VERIFICATION ============
print("\n[1] POSTGRESQL OUTPUTS")
print("-" * 80)
try:
    conn = psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        user=settings.postgres_user,
        password=settings.postgres_password,
        dbname=settings.postgres_db,
    )
    cur = conn.cursor()
    
    tables = [
        ("f2.bin_current_state", "Pipeline 1: Bin Telemetry"),
        ("f2.zone_snapshots", "Pipeline 2: Zone Aggregation"),
        ("f2.route_plans", "Pipeline 3/4: Routes (if any)"),
    ]
    
    for table, desc in tables:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        status = "✓" if count > 0 else "✗"
        print(f"{status} {table:<30} {count:>6} rows  ({desc})")
    
    conn.close()
except Exception as e:
    print(f"✗ PostgreSQL connection failed: {e}")

# ============ KAFKA VERIFICATION ============
print("\n[2] KAFKA TOPIC OUTPUTS")
print("-" * 80)

def make_consumer(topic):
    kwargs = {
        'bootstrap_servers': settings.kafka_bootstrap_servers,
        'auto_offset_reset': 'earliest',
        'enable_auto_commit': True,
        'consumer_timeout_ms': 3000,
        'value_deserializer': lambda v: v.decode('utf-8') if v is not None else None,
    }
    if settings.kafka_username and settings.kafka_password:
        kwargs.update({
            'security_protocol': settings.kafka_security_protocol,
            'sasl_mechanism': settings.kafka_sasl_mechanism,
            'sasl_plain_username': settings.kafka_username,
            'sasl_plain_password': settings.kafka_password,
        })
    return KafkaConsumer(topic, **kwargs)

kafka_topics = [
    (settings.kafka_output_topic, "Pipeline 1: waste.bin.processed"),
    (settings.kafka_zone_output_topic, "Pipeline 2: waste.zone.statistics"),
    (settings.kafka_vehicle_deviation_topic, "Pipeline 3: waste.vehicle.deviation"),
    (settings.kafka_vehicle_location_topic, "Pipeline 4: waste.vehicle.location"),
]

for topic, desc in kafka_topics:
    try:
        consumer = make_consumer(topic)
        total = 0
        sample = None
        try:
            for msg in consumer:
                total += 1
                if sample is None:
                    sample = json.loads(msg.value) if msg.value else None
        finally:
            consumer.close()
        
        status = "✓" if total > 0 else "✗"
        print(f"{status} {topic:<35} {total:>6} msgs  ({desc})")
        if sample:
            print(f"  Sample: {str(sample)[:100]}...")
    except Exception as e:
        print(f"✗ {topic:<35} ERROR: {e}")

# ============ INFLUXDB VERIFICATION ============
print("\n[3] INFLUXDB MEASUREMENTS")
print("-" * 80)

try:
    client = InfluxDBClient(
        url=settings.influx_url,
        token=settings.influx_token,
        org=settings.influx_org
    )
    query_api = client.query_api()
    
    measurements = [
        (settings.influx_raw_bucket, "bin_readings_raw", "Pipeline 1: Raw bin readings"),
        (settings.influx_processed_bucket, "bin_readings_processed", "Pipeline 1: Processed readings"),
        (settings.influx_zone_bucket, "zone_statistics", "Pipeline 2: Zone aggregations"),
        (settings.influx_vehicle_bucket, "vehicle_positions", "Pipeline 4: Vehicle positions"),
    ]
    
    for bucket, measurement, desc in measurements:
        flux = f'from(bucket:"{bucket}") |> range(start: -24h) |> filter(fn: (r) => r._measurement == "{measurement}") |> count(column: "_value")'
        total = 0
        try:
            tables = query_api.query(query=flux, org=settings.influx_org)
            for table in tables:
                for record in table.records:
                    value = record.get_value()
                    if isinstance(value, int):
                        total += value
        except Exception as e:
            print(f"✗ {bucket:<30} {measurement:<25} ERROR: {e}")
            continue
        
        status = "✓" if total > 0 else "✗"
        print(f"{status} {bucket:<30} {measurement:<25} {total:>8} points  ({desc})")
    
    client.close()
except Exception as e:
    print(f"✗ InfluxDB connection failed: {e}")

# ============ SUMMARY ============
print("\n" + "=" * 80)
print("INTERPRETATION:")
print("=" * 80)
print("""
✓ = Output detected (pipeline working)
✗ = No output detected (pipeline not working or not tested)

Pipeline 1 (Bin Telemetry):
  - Should write to: bin_current_state (Postgres), bin_readings_raw/processed (InfluxDB), waste.bin.processed (Kafka)

Pipeline 2 (Zone Aggregation):
  - Should write to: zone_snapshots (Postgres), zone_statistics (InfluxDB), waste.zone.statistics (Kafka)

Pipeline 3 (Vehicle Deviation):
  - Should write to: waste.vehicle.deviation (Kafka)

Pipeline 4 (Vehicle Position):
  - Should write to: vehicle_positions (InfluxDB), waste.vehicle.location (Kafka)
""")
