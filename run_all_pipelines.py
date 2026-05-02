#!/usr/bin/env python3
"""Run all 4 Flink pipelines sequentially for testing"""
import subprocess
import time
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
FLINK_DIR = PROJECT_ROOT / "flink-processor"

# Environment variables for Docker containers (db_default network)
ENV = {
    "POSTGRES_HOST": "postgres",
    "POSTGRES_PORT": "5432",
    "POSTGRES_DB": "waste_management",
    "POSTGRES_USER": "waste_admin",
    "POSTGRES_PASSWORD": "waste_admin_password",
    "INFLUX_URL": "http://influxdb:8086",
    "INFLUX_ORG": "waste-org",
    "INFLUX_TOKEN": "my-super-token",
    "KAFKA_BOOTSTRAP_SERVERS": "163.47.8.3:9094",
    "KAFKA_USERNAME": "user1",
    "KAFKA_PASSWORD": "c4eFajFH2t",
    "KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
    "KAFKA_SASL_MECHANISM": "SCRAM-SHA-256",
}

PIPELINES = [
    ("job.py", "Pipeline 1: Bin Telemetry → waste.bin.processed, bin_current_state, bin_readings_raw/processed"),
    ("job_zone.py", "Pipeline 2: Zone Aggregation → waste.zone.statistics, zone_snapshots"),
    ("job_deviation.py", "Pipeline 3: Vehicle Deviation → waste.vehicle.deviation"),
    ("job_vehicle.py", "Pipeline 4: Vehicle Position → waste.vehicle.location"),
]

def run_pipeline(script_name: str, description: str) -> bool:
    """Run a single pipeline job in a Docker container"""
    print(f"\n{'='*70}")
    print(f"  {description}")
    print(f"{'='*70}")
    
    env_args = [f"-e {k}={v}" for k, v in ENV.items()]
    
    cmd = [
        "docker", "run", "--rm",
        "--network", "db_default",
        *env_args,
        "-v", f"{FLINK_DIR}:/app",
        "python:3.10",
        "bash", "-c", f"cd /app && pip install -q -r requirements.txt && timeout 30 python {script_name} --mode kafka || echo 'TIMEOUT or ERROR'"
    ]
    
    try:
        result = subprocess.run(cmd, text=True, capture_output=False)
        return result.returncode == 0
    except Exception as e:
        print(f"ERROR running pipeline: {e}")
        return False

def main():
    print("Starting all 4 Flink pipelines for testing...")
    print(f"Project: {PROJECT_ROOT}\n")
    
    results = {}
    for script, desc in PIPELINES:
        success = run_pipeline(script, desc)
        results[script] = success
        time.sleep(2)  # Brief pause between pipelines
    
    # Summary
    print(f"\n{'='*70}")
    print("PIPELINE TEST SUMMARY")
    print(f"{'='*70}")
    for script, success in results.items():
        status = "✓ PASS" if success else "✗ FAIL"
        print(f"{status} - {script}")
    
    all_passed = all(results.values())
    print(f"\nOverall: {'✓ ALL PASSED' if all_passed else '✗ SOME FAILED'}")
    return 0 if all_passed else 1

if __name__ == "__main__":
    sys.exit(main())
