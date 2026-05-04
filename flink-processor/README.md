# Flink Processor Integration Guide

This module runs four streaming pipelines together for the Smart Waste Management System data analysis flow.

## Service Overview

The Flink processor consumes bin telemetry and vehicle location events, enriches data with PostgreSQL metadata, publishes derived Kafka events, and writes time-series metrics to InfluxDB.

Pipelines:

1. Pipeline 1 - Bin Telemetry Processor (`job.py`)
2. Pipeline 2 - Zone Aggregation (`job_zone.py`)
3. Pipeline 3 - Vehicle Deviation Detector (`job_deviation.py`)
4. Pipeline 4 - Vehicle Position Historian (`job_vehicle.py`)

## Shared Configuration Check

All four pipelines import the same `load_settings()` from `config.py`, so they share one `.env` source for:

- Kafka broker/auth configuration
- PostgreSQL connection
- InfluxDB connection and bucket names
- Topic names

Required topics used across the integrated flow:

- `waste.bin.telemetry`
- `waste.bin.processed`
- `waste.zone.statistics`
- `waste.vehicle.location`
- `waste.vehicle.deviation`

## Pipeline IO Summary

### Input Topics

- Pipeline 1: `waste.bin.telemetry`
- Pipeline 2: `waste.bin.processed`
- Pipeline 3: `waste.vehicle.location`
- Pipeline 4: `waste.vehicle.location`

### Output Topics

- Pipeline 1: `waste.bin.processed`
- Pipeline 2: `waste.zone.statistics`
- Pipeline 3: `waste.vehicle.deviation`
- Pipeline 4: no Kafka output (writes InfluxDB only)

## PostgreSQL Tables Used

From schema in `../db/init.sql`:

- Read: `bins`, `city_zones`, `waste_categories` (pipeline 1 metadata), `route_plans` (pipeline 3 route lookup)
- Write: `bin_current_state` (pipeline 1), `zone_snapshots` (pipeline 2)

## InfluxDB Measurements Used

- `bin_readings_raw`
- `bin_readings_processed`
- `zone_statistics`
- `vehicle_positions`

Default bucket mapping in `.env` / `.env.example`:

- `INFLUX_RAW_BUCKET=bin_readings_raw`
- `INFLUX_PROCESSED_BUCKET=bin_readings_processed`
- `INFLUX_ZONE_BUCKET=zone_statistics`
- `INFLUX_VEHICLE_BUCKET=vehicle_positions`

## Environment Variables

Core variables:

- `APP_ENV`, `LOG_LEVEL`
- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_INPUT_TOPIC`
- `KAFKA_OUTPUT_TOPIC`
- `KAFKA_ZONE_INPUT_TOPIC`
- `KAFKA_ZONE_OUTPUT_TOPIC`
- `KAFKA_VEHICLE_LOCATION_TOPIC`
- `KAFKA_VEHICLE_DEVIATION_TOPIC`
- `KAFKA_SECURITY_PROTOCOL`, `KAFKA_SASL_MECHANISM`, `KAFKA_USERNAME`, `KAFKA_PASSWORD`
- `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`
- `INFLUX_URL`, `INFLUX_ORG`, `INFLUX_TOKEN`, `INFLUX_ENABLED`
- `INFLUX_RAW_BUCKET`, `INFLUX_PROCESSED_BUCKET`, `INFLUX_ZONE_BUCKET`, `INFLUX_VEHICLE_BUCKET`

Setup:

```bash
cp .env.example .env
```

## Docker Run (All Pipelines Together)

To avoid changing the root `docker-compose.yml`, this module includes a dedicated integration stack:

- Compose file: `flink-processor/docker-compose.integration.yml`

From repository root:

```bash
docker compose -f flink-processor/docker-compose.integration.yml up --build
```

This starts:

- Kafka
- PostgreSQL (with `db/init.sql`)
- InfluxDB + bucket setup script
- Flink Pipeline 1, 2, 3, and 4 containers

Stop and remove:

```bash
docker compose -f flink-processor/docker-compose.integration.yml down -v
```

## Local Run (Without Docker for Flink Code)

You can run services in Docker and execute pipeline scripts locally.

Install dependencies:

```bash
pip install -r flink-processor/requirements.txt
```

Run each pipeline (from `flink-processor/`):

```bash
python job.py --mode kafka
python job_zone.py --mode kafka
python job_deviation.py --mode kafka
python job_vehicle.py --mode kafka
```

## Tests

Run unit tests:

```bash
cd flink-processor
pytest -q
```

## End-to-End Scripts

Scripts are in `flink-processor/tests/e2e/`.

### 1. Send bin telemetry scenarios

```bash
python flink-processor/tests/e2e/send_bin_telemetry.py
```

Scenarios published:

- normal bin
- monitor bin
- urgent bin
- critical bin
- low battery bin
- rapid filling scenario (start + spike events)
- possible tampering scenario (weak signal + abnormal temperature)

### 2. Send vehicle location scenarios

```bash
python flink-processor/tests/e2e/send_vehicle_location.py
```

Scenarios published:

- normal route-following GPS point
- deviated GPS point
- deviation lasting more than 2 minutes

### 3. Verify outputs

```bash
python flink-processor/tests/e2e/verify_outputs.py
```

Verification covers:

- Kafka topics:
  - `waste.bin.processed`
  - `waste.zone.statistics`
  - `waste.vehicle.deviation`
- PostgreSQL tables:
  - `bin_current_state`
  - `zone_snapshots`
- Influx measurements:
  - `bin_readings_raw`
  - `bin_readings_processed`
  - `zone_statistics`
  - `vehicle_positions`

If Kafka/PostgreSQL/Influx is configured as disabled in `.env` (for example empty/disabled host or `INFLUX_ENABLED=false`), verification logs that check as skipped instead of hard failing.

## F2 Flink Stream Processor Demo Script

Use this flow when presenting the real-time processor end to end.

### 1. Start the system

```bash
docker compose up --build
```

Say:

> This starts the Flink processor with Kafka, PostgreSQL, and InfluxDB connections. The service listens to real-time bin and vehicle topics.

### 2. Send bin telemetry events

```bash
python flink-processor/tests/e2e/send_bin_telemetry.py
```

Say:

> I am sending sample smart-bin sensor readings into the Kafka topic waste.bin.telemetry.

### 3. Show processed bin output

Check Kafka topic:

```bash
python flink-processor/tests/e2e/verify_outputs.py --topic waste.bin.processed
```

Say:

> Pipeline 1 processes the raw telemetry, enriches it with bin metadata, calculates estimated waste weight, fill rate, predicted full time, and applies the weighted priority score model.

Show fields:

```text
bin_id
fill_level_pct
estimated_weight_kg
priority_score
status
alerts
```

### 4. Show PostgreSQL bin state update

```sql
SELECT * FROM bin_current_state;
```

Say:

> The latest state of every bin is upserted into PostgreSQL, so the system always has the current bin status.

### 5. Show zone aggregation

```bash
python flink-processor/tests/e2e/verify_outputs.py --topic waste.zone.statistics
```

Say:

> Pipeline 2 groups processed bin events by zone and calculates zone-level statistics such as average fill level, urgent bin count, critical bin count, and total estimated weight.

Also check:

```sql
SELECT * FROM zone_snapshots;
```

### 6. Send vehicle GPS events

```bash
python flink-processor/tests/e2e/send_vehicle_location.py
```

Say:

> I am now sending garbage truck GPS readings into the Kafka topic waste.vehicle.location.

### 7. Show vehicle positions in InfluxDB

Say:

> Pipeline 4 stores every vehicle GPS ping in InfluxDB under the vehicle_positions measurement for historical tracking.

Check measurement:

```text
vehicle_positions
```

### 8. Show route deviation alert

```bash
python flink-processor/tests/e2e/verify_outputs.py --topic waste.vehicle.deviation
```

Say:

> Pipeline 3 compares the vehicle’s current GPS location against its planned route. If the vehicle stays more than 500 meters away from the route for over 2 minutes, it publishes a deviation alert.

Expected alert:

```json
{
  "vehicle_id": "LORRY-01",
  "job_id": "JOB-001",
  "deviation_m": 650.4,
  "duration_s": 150
}
```

### 9. Explain weighted priority score

Say:

> Instead of using only fill level, our system calculates a weighted priority score using fill level, time since last collection, predicted fill condition, distance cost, and risk factor.

Formula:

```text
Priority Score = w1F + w2T + w3P + w4D + w5R
```

Status mapping:

```text
0–30     normal
30–60    monitor
60–85    urgent
85–100   critical
```

### 10. Final summary

Say:

> This completes the real-time intelligence layer of the smart waste system. The processor consumes raw IoT and vehicle data, enriches it, stores operational state, produces analytics, and publishes events for dashboards and downstream services.

## Expected Demo Flow

1. Start Docker services.
2. Send bin telemetry sample events.
3. Confirm processed bin events in `waste.bin.processed`.
4. Confirm `bin_current_state` updated.
5. Confirm zone statistics are created.
6. Send vehicle location events.
7. Confirm `vehicle_positions` are written.
8. Send deviated vehicle GPS points.
9. Confirm `waste.vehicle.deviation` alert is published.

## Troubleshooting

- No Kafka output observed:
  - Check broker endpoint (`KAFKA_BOOTSTRAP_SERVERS`) and topic names in `.env`.
  - Verify all pipeline containers are running and healthy.

- PostgreSQL connection errors:
  - Confirm `POSTGRES_HOST`, port, database, user, password.
  - Ensure schema was initialized from `db/init.sql`.

- Influx writes missing:
  - Check `INFLUX_ENABLED=true`.
  - Confirm token/org/url values and bucket names.
  - Validate bucket setup completed via `influxdb-setup` service.

- Vehicle deviation alert not emitted:
  - Ensure route plan exists for the same `vehicle_id` and `job_id` used by E2E GPS events.
  - Send at least one deviated point and another deviated point more than 2 minutes later.

- Zone snapshots not created:
  - Ensure pipeline 1 is producing `waste.bin.processed` and pipeline 2 is consuming the same topic.
