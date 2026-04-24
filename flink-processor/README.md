# Flink Processor - Pipeline 1 (Bin Telemetry)

## Scope

This service will implement Pipeline 1 only:

Kafka -> Processing -> PostgreSQL + InfluxDB + Kafka

Input topic:

- waste.bin.telemetry

Output topic:

- waste.bin.processed

Schema source of truth:

- ../db/init.sql

## Phase 1 Status

Completed in this phase:

- Project structure scaffolded
- Environment/config loading implemented
- Entry point created with mode argument (kafka/local)
- Placeholder classes created for processor and sinks

Not implemented yet:

- Kafka parsing and validation
- PostgreSQL metadata enrichment
- Computation and anomaly logic
- InfluxDB/PostgreSQL/Kafka sink operations
- Full PyFlink stream wiring

## Phase 2 Status

Completed in this phase:

- JSON parsing from Kafka messages with nested payload extraction
- Validation for all required fields: bin_id, fill_level_pct, battery_level_pct, timestamp
- Type checking and range validation (0-100 for numeric percentages)
- ISO 8601 timestamp parsing with timezone support
- Comprehensive test suite: 22 tests, all passing
  - Valid event parsing
  - Extra optional fields handling
  - Missing field detection
  - Type validation
  - Range validation
  - Timestamp format validation

Not implemented yet:

- PostgreSQL metadata enrichment
- Computation and anomaly logic
- InfluxDB/PostgreSQL/Kafka sink operations
- Full PyFlink stream wiring

## Phase 3 Status

Completed in this phase:

- PostgreSQL metadata enrichment using schema in ../db/init.sql
- Bin lookup from bins + waste_categories (connection pool + cache)
- Metadata test coverage with mocked DB pool

## Phase 4 Status

Completed in this phase:

- Processor enrichment flow via metadata store in process()
- Estimated weight calculation: fill*level_pct * volume*litres * avg_kg_per_litre
- Urgency classification implemented:
  - fill_level_pct < 50 -> normal
  - 50 <= fill_level_pct < 75 -> monitor
  - 75 <= fill_level_pct <= 90 -> urgent
  - fill_level_pct > 90 -> critical
- urgency_score set from fill_level_pct (rounded to nearest integer)
- Unit tests added for status boundaries and computed fields

Not implemented yet:

- InfluxDB/PostgreSQL/Kafka sink operations
- Full PyFlink stream wiring

## Phase 5 Status

Completed in this phase:

- Anomaly detection added in processing output
  - low_battery when battery_level_pct < 20
  - weak_signal when optional signal_strength < -100 dBm
  - abnormal_temperature when optional temperature_c is outside [-20, 70]
- Output now includes anomaly_detected and anomaly_flags
- Unit tests added for anomaly scenarios

Not implemented yet:

- InfluxDB/PostgreSQL/Kafka sink operations
- Full PyFlink stream wiring

## Phase 6 Status

Completed in this phase:

- Kafka sink implemented to publish processed events to waste.bin.processed
- PostgreSQL sink implemented with UPSERT to bin_current_state using real schema columns
- Influx sink implemented for both raw and processed measurements
  - raw -> bucket bin_readings_raw, measurement bin_readings_raw
  - processed -> bucket bin_readings_processed, measurement bin_readings_processed
- Added mock-based sink tests that run without external services

Not implemented yet:

- Full PyFlink stream wiring

## Phase 7 Status

Completed in this phase:

- End-to-end orchestration wired in job.py
  - local mode: reads JSONL input file and processes events through all sinks
  - kafka mode: consumes from waste.bin.telemetry and processes continuously
- Added max-messages control for bounded smoke runs in both modes
- Added per-event resilience (logs and continues on bad records)
- Added graceful shutdown for all clients/pools (Kafka, PostgreSQL, Influx, metadata store)
- Added job orchestration unit tests (local reader + event fan-out)

Not implemented yet:

- Full PyFlink DataStream API execution semantics

## Phase 8 Status

Completed in this phase:

- Added optional pyflink-local mode in job.py using PyFlink DataStream API
  - reads local JSONL events into a bounded stream
  - executes processing fan-out through Influx, PostgreSQL, and Kafka via map function
- Added helper for bounded local JSON event loading for DataStream execution
- Improved main() resource lifecycle with lazy per-mode initialization and safe cleanup
- Added tests for pyflink-local CLI mode and bounded JSON loading helper

Pipeline 1 implementation status:

- COMPLETE for requested pipeline behavior (ingest, enrich, compute, anomaly, sink fan-out, runtime modes)

## Folder Structure

flink-processor/

- job.py
- config.py
- models.py
- metadata_store.py
- sinks/
  - influx_sink.py
  - postgres_sink.py
  - kafka_sink.py
- processors/
  - bin_telemetry.py
- tests/
  - test_bin_telemetry.py
- requirements.txt
- Dockerfile
- .env.example
- README.md

## Configuration

Copy .env.example to .env and adjust values for your environment.

Key groups:

- Kafka: bootstrap server, topics, SASL settings
- PostgreSQL: host, port, db, user, password
- InfluxDB: url, org, token, raw/processed buckets

## Local Smoke Check

Install dependencies:

python -m pip install -r requirements.txt

Run unit test scaffold:

python -m pytest -q

Run job scaffold:

python job.py --mode kafka

Optional bounded local smoke run:

python job.py --mode local --max-messages 10

Optional PyFlink DataStream local run:

python job.py --mode pyflink-local --max-messages 10

Expected output:

- Logs indicate active pipeline processing for selected mode
