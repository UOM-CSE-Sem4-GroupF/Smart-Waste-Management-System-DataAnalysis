#!/usr/bin/env python3
"""Final verification of all 4 pipelines outputs using Docker"""
import subprocess
import json
import time

def run_in_docker(cmd, network="db_default"):
    """Execute a command in a Docker container on db_default network"""
    docker_cmd = [
        "docker", "run", "--rm",
        "--network", network,
        "-e", "POSTGRES_HOST=postgres",
        "-e", "POSTGRES_PORT=5432",
        "-e", "POSTGRES_DB=waste_management",
        "-e", "POSTGRES_USER=waste_admin",
        "-e", "POSTGRES_PASSWORD=waste_admin_password",
        "-e", "INFLUX_URL=http://influxdb:8086",
        "-e", "INFLUX_ORG=waste-org",
        "-e", "INFLUX_TOKEN=my-super-token",
        "-e", "KAFKA_BOOTSTRAP_SERVERS=163.47.8.3:9094",
        "-e", "KAFKA_USERNAME=user1",
        "-e", "KAFKA_PASSWORD=c4eFajFH2t",
        "-e", "KAFKA_SECURITY_PROTOCOL=SASL_SSL",
        "-e", "KAFKA_SASL_MECHANISM=SCRAM-SHA-256",
        "python:3.10",
        "bash", "-c", cmd
    ]
    result = subprocess.run(docker_cmd, capture_output=True, text=True, timeout=30)
    return result.stdout, result.stderr, result.returncode

print("=" * 90)
print("ALL-PIPELINES COMPREHENSIVE VERIFICATION")
print("=" * 90)

# ============ POSTGRES CHECK ============
print("\n[1] POSTGRESQL VERIFICATION")
print("-" * 90)

postgres_check = """
pip install -q psycopg2-binary && python3 << 'PYEOF'
import psycopg2
try:
    conn = psycopg2.connect(host='postgres', port=5432, user='waste_admin', password='waste_admin_password', dbname='waste_management')
    cur = conn.cursor()
    
    queries = [
        ("f2.bin_current_state", "Pipeline 1: Bin Telemetry"),
        ("f2.zone_snapshots", "Pipeline 2: Zone Aggregation"),
    ]
    
    for table, desc in queries:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        status = "✓" if count > 0 else "✗"
        print(f"{status} {table:<30} {count:>8} rows  ({desc})")
    
    conn.close()
except Exception as e:
    print(f"✗ PostgreSQL ERROR: {e}")
PYEOF
"""

stdout, stderr, code = run_in_docker(postgres_check)
print(stdout)
if stderr and "warning" not in stderr.lower():
    print(f"STDERR: {stderr}")

# ============ INFLUXDB CHECK ============
print("\n[2] INFLUXDB VERIFICATION")
print("-" * 90)

influx_check = """
pip install -q influxdb-client && python3 << 'PYEOF'
from influxdb_client import InfluxDBClient
try:
    client = InfluxDBClient(url='http://influxdb:8086', token='my-super-token', org='waste-org')
    query_api = client.query_api()
    
    measurements = [
        ('bin_readings_raw', 'bin_readings_raw', 'Pipeline 1: Raw bin readings'),
        ('bin_readings_processed', 'bin_readings_processed', 'Pipeline 1: Processed readings'),
        ('zone_statistics', 'zone_statistics', 'Pipeline 2: Zone aggregations'),
        ('vehicle_positions', 'vehicle_positions', 'Pipeline 4: Vehicle positions'),
    ]
    
    for bucket, measurement, desc in measurements:
        flux = f'from(bucket:"{bucket}") |> range(start: -24h) |> filter(fn: (r) => r._measurement == "{measurement}") |> count(column: "_value")'
        total = 0
        try:
            tables = query_api.query(query=flux, org='waste-org')
            for table in tables:
                for record in table.records:
                    value = record.get_value()
                    if isinstance(value, int):
                        total += value
        except:
            pass
        
        status = "✓" if total > 0 else "✗"
        print(f"{status} {bucket:<30} {measurement:<25} {total:>8} points  ({desc})")
    
    client.close()
except Exception as e:
    print(f"✗ InfluxDB ERROR: {e}")
PYEOF
"""

stdout, stderr, code = run_in_docker(influx_check)
print(stdout)
if stderr and "warning" not in stderr.lower():
    print(f"STDERR: {stderr}")

# ============ KAFKA CHECK ============
print("\n[3] KAFKA VERIFICATION")
print("-" * 90)

kafka_check = """
pip install -q kafka-python && python3 << 'PYEOF'
from kafka import KafkaConsumer
import json

def check_topic(topic, desc):
    kwargs = {
        'bootstrap_servers': '163.47.8.3:9094',
        'auto_offset_reset': 'earliest',
        'enable_auto_commit': True,
        'consumer_timeout_ms': 2000,
        'value_deserializer': lambda v: v.decode('utf-8') if v else None,
        'security_protocol': 'SASL_SSL',
        'sasl_mechanism': 'SCRAM-SHA-256',
        'sasl_plain_username': 'user1',
        'sasl_plain_password': 'c4eFajFH2t',
    }
    try:
        consumer = KafkaConsumer(topic, **kwargs)
        total = 0
        try:
            for msg in consumer:
                total += 1
                if total > 100:  # Limit to prevent long runtime
                    break
        finally:
            consumer.close()
        
        status = "✓" if total > 0 else "✗"
        print(f"{status} {topic:<35} {total:>8} msgs  ({desc})")
    except Exception as e:
        print(f"✗ {topic:<35} ERROR: {str(e)[:50]}")

check_topic('waste.bin.processed', 'Pipeline 1: Bin Telemetry')
check_topic('waste.zone.statistics', 'Pipeline 2: Zone Aggregation')
check_topic('waste.vehicle.deviation', 'Pipeline 3: Vehicle Deviation')
check_topic('waste.vehicle.location', 'Pipeline 4: Vehicle Position')
PYEOF
"""

stdout, stderr, code = run_in_docker(kafka_check)
print(stdout)
if stderr and "warning" not in stderr.lower():
    print(f"STDERR: {stderr}")

# ============ SUMMARY ============
print("\n" + "=" * 90)
print("FINAL ASSESSMENT")
print("=" * 90)
print("""
Based on the verification results:
✓ = Pipeline CONFIRMED working (outputs detected)
✗ = Pipeline NOT working or not fully tested (no outputs detected)

NEXT STEPS:
1. If all 4 pipelines show ✓, the system is READY FOR SUBMISSION
2. If some pipelines show ✗, investigate the specific pipeline job code
3. If infrastructure (Postgres/Influx/Kafka) shows ✗, check connectivity
""")
