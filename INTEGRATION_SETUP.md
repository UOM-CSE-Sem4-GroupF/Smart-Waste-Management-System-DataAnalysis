# Smart Waste Management System — Data Analysis Layer (Group F2)

**Version:** 1.0  
**Status:** ✅ Complete Integration Setup  
**Architecture:** Docker Compose (development) / Kubernetes (production)

---

## Quick Start

### Prerequisites
- Docker & Docker Compose
- 8GB RAM minimum
- .env file (copy from template if needed)

### Start the System

**On Linux/Mac:**
```bash
chmod +x start.sh
./start.sh apps    # Start all application services
```

**On Windows (PowerShell):**
```powershell
.\start.ps1 -Profile apps
```

**Manual Docker Compose:**
```bash
docker-compose up -d --build
```

---

## System Architecture

This folder contains all **Group F2** microservices for the Smart Waste Management System:

### Data Pipeline Flow

```
Sensor Telemetry (Kafka)
    ↓
Flink Stream Processor (Real-time)
    ├→ PostgreSQL (bin_current_state)
    ├→ InfluxDB (time-series)
    └→ Kafka (waste.bin.processed)
    ↓
Route Optimizer (OR-Tools)
    └→ Kafka (waste.routes.optimized) + PostgreSQL (route_plans)
    ↓
ML Service (FastAPI)
    ├← MLflow (Model Registry)
    ├← PostgreSQL (Historical data)
    └→ APIs (/predict/fill-time, /trends/waste-generation)
    ↓
Airflow Orchestrator (Batch)
    ├→ Spark (Analytics)
    ├→ MLflow (Model Training)
    └→ Kafka (waste.model.retrained, waste.routine.schedule.trigger)
```

---

## Services

| Service | Port | Purpose | Status |
|---------|------|---------|--------|
| **Kafka** | 9092 | Event streaming (messaging backbone) | ✅ Running |
| **Zookeeper** | 2181 | Kafka cluster coordination | ✅ Running |
| **PostgreSQL (Waste)** | 5432 | F2 schema (bins, vehicles, routes, zones) | ✅ Running |
| **PostgreSQL (Airflow)** | 5432 | Airflow metadata (separate instance) | ✅ Running |
| **InfluxDB** | 8086 | Time-series metrics (bin levels, vehicle positions) | ✅ Running |
| **MLflow** | 5000 | Model registry & experiment tracking | ✅ Running |
| **ML Service** | 8000 | FastAPI prediction server | ✅ Running |
| **Flink Processor** | N/A | Stream processing (Kafka consumer) | ✅ Running |
| **Route Optimizer** | N/A | OR-Tools route computation | ✅ Running |
| **Airflow** | 8080 | Batch orchestration & scheduling | ✅ Running |
| **Spark** (optional) | 8081 | Large-scale batch analytics | Optional |
| **App Consumer** (test) | N/A | Test Kafka consumer | Optional |

---

## Environment Configuration

### Default `.env` Values

```bash
# Kafka (inside docker: kafka:29092, outside: localhost:9092)
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
KAFKA_SECURITY_PROTOCOL=PLAINTEXT

# PostgreSQL Waste Management
POSTGRES_HOST=postgres-waste
POSTGRES_DB=waste_management
POSTGRES_USER=waste_admin
POSTGRES_PASSWORD=waste_admin_password

# PostgreSQL Airflow
POSTGRES_AIRFLOW_HOST=postgres-airflow
POSTGRES_AIRFLOW_USER=airflow
POSTGRES_AIRFLOW_PASSWORD=airflow_secure_password

# InfluxDB
INFLUX_URL=http://influxdb:8086
INFLUX_ORG=waste-org
INFLUX_TOKEN=my-super-token

# MLflow
MLFLOW_TRACKING_URI=http://mlflow:5000

# ML Service
ML_SERVICE_URL=http://ml-service:8000
```

See `.env` file for all configuration options.

---

## Access Points

### Development UIs

- **Airflow Dashboard**: http://localhost:8080 (username: `admin`, password: `admin`)
- **MLflow**: http://localhost:5000
- **ML Service API Docs**: http://localhost:8000/docs

### Database Connections

```bash
# PostgreSQL Waste Management
psql -h localhost -U waste_admin -d waste_management -p 5432

# PostgreSQL Airflow
psql -h localhost -U airflow -d airflow -p 5432

# InfluxDB (web UI)
http://localhost:8086
```

### Kafka Topics

```bash
# List all topics
docker-compose exec kafka kafka-topics --list --bootstrap-server kafka:29092

# Consume from a topic (e.g., waste.bin.telemetry)
docker-compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic waste.bin.telemetry \
  --from-beginning

# Produce a test message
docker-compose exec kafka kafka-console-producer \
  --broker-list kafka:29092 \
  --topic waste.bin.telemetry
```

---

## Data Models

### PostgreSQL F2 Schema

All tables defined in [db/init.sql](db/init.sql):

- `waste_categories` — Waste type metadata (food_waste, paper, glass, plastic, general, e_waste)
- `city_zones` — Geographical zones with collection schedules
- `bins` — Physical bin registry with GPS coordinates
- `bin_current_state` — Latest fill level, urgency score, estimated weight
- `vehicles` — Lorry fleet with capacity limits
- `route_plans` — Pre-computed collection routes
- `zone_snapshots` — Aggregated zone statistics (10-min windows)
- `model_performance` — ML model metrics & retraining history

### InfluxDB Buckets

- `bin_readings_raw` — Raw sensor telemetry (7-day retention)
- `bin_readings_processed` — Enriched & classified readings (90-day)
- `zone_statistics` — Zone aggregations (2-year)
- `vehicle_positions` — GPS trail history (1-year)
- `waste_generation_trends` — Long-term patterns (forever)

---

## Key Files

| File | Purpose |
|------|---------|
| `.env` | Environment configuration (DO NOT commit) |
| `docker-compose.yml` | Complete service orchestration |
| `start.sh` | Linux/Mac startup automation |
| `start.ps1` | Windows PowerShell startup automation |
| `db/init.sql` | PostgreSQL F2 schema initialization |
| `flink-processor/job.py` | Bin telemetry stream processor |
| `route-optimizer/app.py` | OR-Tools route optimization |
| `ml-service/app/main.py` | FastAPI ML service |
| `airflow/dags/main_dag.py` | Batch orchestration workflow |

---

## Common Tasks

### View Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f flink-processor
docker-compose logs -f airflow
docker-compose logs -f ml-service
```

### Stop All Services

```bash
docker-compose down
```

### Reset Everything (including volumes)

```bash
docker-compose down -v
```

### Run Integration Tests

```bash
pytest tests/test_integration_pipeline.py -v
```

### Access Kafka Inside Container

```bash
docker-compose exec kafka /bin/bash
```

### Check Service Status

```bash
docker-compose ps
```

---

## Integration Points

### Kafka Topics (F4-owned, F2-consumed)

| Topic | Producer | F2 Consumer | Purpose |
|-------|----------|------------|---------|
| `waste.bin.telemetry` | F1 EMQX | Flink | Raw sensor readings |
| `waste.bin.processed` | **Flink** | Route-Optimizer | Enriched readings |
| `waste.zone.statistics` | **Flink** | F3 Dashboard | Zone aggregations |
| `waste.vehicle.location` | F1/F3 Flutter | **Flink** | GPS positions |
| `waste.vehicle.deviation` | **Flink** | F3 Notification | Route deviations |
| `waste.routes.optimized` | **Route-Optimizer** | F3 Orchestrator | Optimized routes |
| `waste.job.completed` | F3 Orchestrator | **Spark** | Completed collections |
| `waste.model.retrained` | **Airflow/Spark** | F3 Orchestrator | Model updates |

### REST APIs (Kong-gated for F3)

| Endpoint | Service | Purpose |
|----------|---------|---------|
| `GET /api/v1/ml/predict/fill-time` | **ML Service** | Predict bin full time |
| `GET /api/v1/ml/predict/zone-generation` | **ML Service** | Forecast waste generation |
| `POST /api/v1/ml/score/route` | **ML Service** | Evaluate route efficiency |
| `/internal/models/reload` | **ML Service** | Reload models from MLflow |

---

## Troubleshooting

### Kafka Topics Not Created

```bash
# Verify Kafka is running
docker-compose ps | grep kafka

# Check topics manually
docker-compose exec kafka kafka-topics --list --bootstrap-server kafka:29092

# Create missing topics
docker-compose exec kafka kafka-topics --create --if-not-exists \
  --bootstrap-server kafka:29092 \
  --topic waste.bin.telemetry \
  --partitions 6 \
  --replication-factor 1
```

### PostgreSQL Connection Refused

```bash
# Check PostgreSQL container is running
docker-compose ps | grep postgres-waste

# Check logs
docker-compose logs postgres-waste

# Test connection
docker-compose exec postgres-waste pg_isready -U waste_admin
```

### ML Service Fails to Load Models

MLflow may be unavailable or models not yet registered. This is **expected on first run**. Service automatically falls back to heuristics.

To manually trigger model training:
```bash
docker-compose exec airflow airflow dags trigger nightly_ml_retraining
```

### Flink Processor Not Processing Messages

1. Verify Kafka has messages: `docker-compose logs kafka`
2. Check Flink logs: `docker-compose logs flink-processor`
3. Ensure PostgreSQL is accessible: `docker-compose exec flink-processor psql -h postgres-waste -U waste_admin -d waste_management -c "SELECT COUNT(*) FROM bins;"`

### Permission Denied Errors on macOS

Ensure your user can run Docker:
```bash
sudo chmod 666 /var/run/docker.sock
```

---

## Performance Tuning

### For Development

```bash
# Use lighter Flink configuration (fewer replicas, smaller batch)
FLINK_MAX_MESSAGES=1000
```

### For Production

```bash
# Enable persistence, increase partitions, add replicas
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=3
INFLUXDB_INIT_RETENTION=90d
```

---

## Next Steps

1. ✅ **Verify all services are running**: `docker-compose ps`
2. ✅ **Check Kafka topics exist**: `docker-compose exec kafka kafka-topics --list --bootstrap-server kafka:29092`
3. ✅ **Populate test data** in PostgreSQL (use db/init.sql + seeds)
4. ✅ **Send test sensor data** via Kafka (use app-consumer test)
5. ✅ **Monitor Flink processing** via logs
6. ✅ **Verify database updates** with `SELECT * FROM bin_current_state;`
7. ✅ **Check route optimization** via PostgreSQL `route_plans` table
8. ✅ **Trigger Airflow DAG** for batch processing
9. ✅ **View ML predictions** via API

---

## Contributing

All changes must:
- Be made **only** within Data Analysis folder and sub-folders
- Follow the existing service isolation principle (no direct DB access between services)
- Use environment variables from `.env` (no hardcoding)
- Include proper error handling and fallbacks
- Have corresponding unit/integration tests
- Be committed with `git commit` after testing

---

## Support & Issues

- Check [db/README.md](db/README.md) for database setup
- Check [flink-processor/README.md](flink-processor/README.md) for stream processing
- Check [ml-service/README.md](ml-service/README.md) for ML service
- Check [route-optimizer/README.md](route-optimizer/README.md) for OR-Tools setup
- Check [airflow/README.md](airflow/README.md) for orchestration

---

**Last Updated:** 2026-04-28  
**Maintained by:** Group F2 Data Layer Team
