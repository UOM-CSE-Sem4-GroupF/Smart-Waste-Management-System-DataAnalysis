# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the **Data Analysis Layer (Group F2)** of a Smart Waste Management System. It processes real-time IoT waste bin telemetry through a pipeline of stream processing, route optimization, ML prediction, and batch analytics.

## Commands

### Running the Full Stack
```bash
# Linux/Mac
./start.sh apps

# Windows PowerShell
.\start.ps1 -Profile apps

# Full Docker Compose
docker-compose up -d --build
```

### Running Individual Services Locally
```bash
# Flink pipelines (each pipeline is a separate job)
pip install -r flink-processor/requirements.txt
python flink-processor/job.py --mode kafka            # Pipeline 1: bin telemetry
python flink-processor/job_zone.py --mode kafka       # Pipeline 2: zone aggregation
python flink-processor/job_deviation.py --mode kafka  # Pipeline 3: vehicle deviation
python flink-processor/job_vehicle.py --mode kafka    # Pipeline 4: vehicle position

# ML Service
uvicorn app.main:app --app-dir ml-service --reload --host 0.0.0.0 --port 8000
```

### Testing
```bash
# Unit tests per component
cd flink-processor && pytest -q
pytest ml-service/tests -q

# System integration tests
python system_integration_test.py

# E2E tests for Flink (requires running Kafka)
python flink-processor/tests/e2e/send_bin_telemetry.py
python flink-processor/tests/e2e/send_vehicle_location.py
python flink-processor/tests/e2e/verify_outputs.py

# Flink isolated integration stack
docker compose -f flink-processor/docker-compose.integration.yml up --build
```

## Architecture

### Data Pipeline Flow
```
IoT Sensors → Kafka (waste.bin.telemetry)
    ↓
Flink Stream Processor [4 jobs]
    ├→ PostgreSQL (bin_current_state, zone_snapshots)
    ├→ InfluxDB (time-series metrics)
    └→ Kafka (waste.bin.processed, waste.zone.statistics, waste.vehicle.deviation)
    ↓
Route Optimizer (OR-Tools)
    ├→ PostgreSQL (route_plans)
    └→ Kafka (waste.routes.optimized)
    ↓
ML Service (FastAPI) ← MLflow (model registry)
    └→ REST predictions (/predict/fill-time, /trends/waste-generation, ...)
    ↓
Airflow + Spark [Batch]
    ├→ MLflow (model retraining)
    └→ Kafka (waste.model.retrained, waste.routine.schedule.trigger)
```

### Components

| Service | Entry Point | Role |
|---------|-------------|------|
| **Flink Processor** | `flink-processor/job*.py` | Real-time stream processing with PyFlink 1.20.0 |
| **Route Optimizer** | `route-optimizer/app.py` | OR-Tools vehicle routing triggered by urgent-bin events |
| **ML Service** | `ml-service/app/main.py` | FastAPI REST API for fill-time and trend predictions |
| **Airflow** | `airflow/dags/main_dag.py` | Batch orchestration, Spark jobs, model retraining |
| **Spark** | `spark/job.py` | Historical batch analytics (zone aggregations) |

### Flink Processor Structure
`flink-processor/processors/` — business logic per pipeline (bin_telemetry, zone_aggregation, vehicle_deviation, vehicle_position)
`flink-processor/sinks/` — output writers (kafka_sink, postgres_sink, influx_sink)
`flink-processor/config.py` — environment-based configuration
`flink-processor/metadata_store.py` — bin/zone metadata fetched from PostgreSQL at startup

### ML Service Structure
`ml-service/app/api/routes_ml.py` — endpoint definitions
`ml-service/app/services/predictor.py` — MLflow model loading + inference; falls back to baseline heuristics if MLflow unavailable
`ml-service/app/schemas.py` — Pydantic request/response models

## Infrastructure

### Kafka Topics
| Topic | Flow |
|-------|------|
| `waste.bin.telemetry` | IoT → Flink P1 |
| `waste.bin.processed` | Flink P1 → Flink P2, Route Optimizer |
| `waste.zone.statistics` | Flink P2 → Dashboard |
| `waste.vehicle.location` | GPS → Flink P3/P4 |
| `waste.vehicle.deviation` | Flink P3 → Monitoring |
| `waste.routes.optimized` | Route Optimizer → Drivers |
| `waste.model.retrained` | Airflow → ML Service |

### Databases
- **PostgreSQL**: Relational state — `bin_current_state` (upserted by Flink), `route_plans`, `zone_snapshots`, `model_performance`
- **InfluxDB**: Time-series metrics in buckets `bin_readings_raw`, `bin_readings_processed`, `vehicle_positions`, `zone_statistics`
- **MLflow**: Model registry; ML Service loads the `Production` stage model at startup

### Key Environment Variables
All services are configured via `.env` (root) or per-service `.env` files. See `.env.example` in each component directory.
Critical variables: `KAFKA_BOOTSTRAP_SERVERS`, `POSTGRES_HOST/DB/USER/PASSWORD`, `INFLUX_URL/TOKEN/ORG`, `MLFLOW_TRACKING_URI`

## Key Constraints

- **No cross-service DB access**: each service only reads/writes its own defined tables
- **Kafka schema contract**: JSON message schemas are agreed across services — changes must be coordinated
- **Flink exactly-once**: relies on Flink checkpointing; do not bypass the state backend
- **MLflow fallback**: ML Service has heuristic fallbacks when the model registry is unavailable — keep those in sync with model updates
