# Group F2 — Data Layer (Smart Waste Management)

## Overview
This repository contains all services owned by **Group F2**:
- Real-time processing (Flink)
- Route optimization (OR-Tools)
- ML APIs (FastAPI)
- Batch processing (Airflow + Spark)
- Database setup (PostgreSQL)

## Architecture Flow

Kafka (telemetry)
    ↓
Flink Processor
    ↓
PostgreSQL (bin_current_state)
    ↓
Route Optimizer
    ↓
Kafka (routes.optimized)
    ↓
Airflow + Spark (batch processing)

ML Service provides APIs used by optimizer/dashboard.

---

## Team Members Allocation

| Member | Component |
|--------|----------|
| Member 1 | Flink Processor |
| Member 2 | Database + Docker Infra |
| Member 3 | Route Optimizer |
| Member 4 | ML Service |
| Member 5 | Airflow + Spark |

---

## Rules

- Each member works ONLY inside their folder
- No direct DB access across services except via defined tables
- Use `.env` for all configs
- Kafka message formats must follow agreed schema