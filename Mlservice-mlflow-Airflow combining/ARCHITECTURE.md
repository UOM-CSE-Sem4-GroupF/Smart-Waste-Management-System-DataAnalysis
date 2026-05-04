# MLflow + ml-service + Airflow Integration Architecture

## Overview

This document describes the three-tier architecture that connects MLflow (central model hub), ml-service (prediction server), and Airflow (orchestration layer) for a complete end-to-end machine learning pipeline.

## Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────┐
│                    MLflow Server (Central Hub)                    │
│                                                                    │
│  ┌─────────────────────┐  ┌──────────────────┐  ┌─────────────┐  │
│  │  Experiments        │  │ Model Registry   │  │ Artifacts   │  │
│  │  (Metrics/Logs)     │  │ (Models+Stages)  │  │ Store       │  │
│  │                     │  │                  │  │             │  │
│  │ • training_metrics  │  │ • fill-time-model│  │ • Model     │  │
│  │ • validation_acc    │  │ • zone-gen-model │  │   artifacts │  │
│  │ • model_size        │  │ • route-model    │  │ • Metrics   │  │
│  └─────────────────────┘  └──────────────────┘  └─────────────┘  │
│           ▲                        ▲                     │         │
│           │                        │                     │         │
└───────────┼────────────────────────┼─────────────────────┼─────────┘
            │ logs metrics/          │ registers and       │ stores
            │ parameters             │ promotes models     │ trained models
            │                        │                     │
    ┌───────┴──────────────┐  ┌─────┴────────────────┐   └──────────┐
    │                      │  │                      │              │
    │   Airflow DAG        │  │   ml-service         │              │
    │                      │  │   (FastAPI)          │              │
    │ ┌──────────────────┐ │  │ ┌──────────────────┐ │              │
    │ │ Spark Training   │ │  │ │ Load Production  │ │              │
    │ │ Job              │ │  │ │ Models at Startup│ │              │
    │ │ (Logs metrics    │ │  │ │ (or on reload)   │ │              │
    │ │  & saves model)  │ │  │ │                  │ │              │
    │ └────────┬─────────┘ │  │ │ Serve Predictions│ │              │
    │          │            │  │ │ /api/v1/ml/*    │ │              │
    │ ┌────────▼─────────┐ │  │ │                  │ │              │
    │ │ Log to MLflow    │ │  │ │ Reload Endpoint  │ │              │
    │ │ Experiment       │ │  │ │ /internal/models/│ │              │
    │ └────────┬─────────┘ │  │ │ reload           │ │              │
    │          │            │  │ │ (called by       │ │              │
    │ ┌────────▼─────────┐ │  │ │  Airflow)        │ │              │
    │ │ Register Model   │ │  │ │                  │ │              │
    │ │ in Registry      │ │  │ │ Log Prediction   │ │              │
    │ └────────┬─────────┘ │  │ │ Metrics to       │ │              │
    │          │            │  │ │ MLflow           │ │              │
    │ ┌────────▼─────────┐ │  │ └──────────────────┘ │              │
    │ │ Promote to       │ │  │                      │              │
    │ │ Production Stage │ │  │                      │              │
    │ └────────┬─────────┘ │  │                      │              │
    │          │            │  │                      │              │
    │ ┌────────▼─────────┐ │  │                      │              │
    │ │ Notify ml-service│ │  │                      │              │
    │ │ to reload models │───►│                      │              │
    │ │ (POST /internal) │ │  │                      │              │
    │ └────────┬─────────┘ │  │                      │              │
    │          │            │  │                      │              │
    │ ┌────────▼─────────┐ │  │                      │              │
    │ │ Publish Kafka    │ │  │                      │              │
    │ │ Event:           │ │  │                      │              │
    │ │ model.retrained  │ │  │                      │              │
    │ └──────────────────┘ │  │                      │              │
    │                      │  │                      │              │
    └──────────────────────┘  └──────────────────────┘              │
                                                                    │
                                ┌──────────────────────────────────┘
                                │
                         ┌──────▼────────┐
                         │  Kafka Events │
                         │               │
                         │ waste.model.  │
                         │ retrained     │
                         └───────────────┘
```

## Three-Tier Architecture

### 1. MLflow Server (Model Hub)
**Purpose**: Central model registry and experiment tracking

**Responsibilities**:
- Track training experiments (metrics, parameters, artifacts)
- Store and version trained models
- Manage model stages (Dev → Staging → Production)
- Provide REST API for model queries

**Key Components**:
- Tracking Server: Records experiments, runs, metrics
- Model Registry: Stores models with version control
- Artifact Store: Stores model files and artifacts

**Technology**: MLflow 2.0+

---

### 2. Airflow (Orchestration Layer)
**Purpose**: Orchestrate the ML training pipeline

**Pipeline Flow**:
```
run_spark_job 
    ↓
log_metrics_to_mlflow 
    ↓
register_model_in_mlflow 
    ↓
promote_model_to_production 
    ↓
notify_ml_service_reload 
    ↓
publish_kafka_event
```

**Key Tasks**:

1. **run_spark_job** (BashOperator)
   - Executes Spark training job in container
   - Training code logs metrics to MLflow
   - Saves trained model artifacts

2. **log_metrics_to_mlflow** (PythonOperator)
   - Logs training metrics to MLflow experiment
   - Records parameters used in training
   - Example metrics: accuracy, validation loss, model size

3. **register_model_in_mlflow** (PythonOperator)
   - Registers trained model in MLflow Model Registry
   - Assigns version number (v1, v2, v3...)
   - Links model to training run artifacts

4. **promote_model_to_production** (PythonOperator)
   - Transitions latest model to "Production" stage
   - Archives previous production models
   - Makes model discoverable by ml-service

5. **notify_ml_service_reload** (PythonOperator)
   - Calls `POST /internal/models/reload` on ml-service
   - Triggers immediate model loading from MLflow
   - Confirms successful reload with response

6. **publish_kafka_event** (PythonOperator)
   - Publishes `model.retrained` event to Kafka
   - Notifies downstream services of model update
   - Used for alerting and monitoring

**Environment Variables**:
```
MLFLOW_TRACKING_URI=http://mlflow:5000
ML_SERVICE_URL=http://ml-service:8000
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql://...
```

**Technology**: Apache Airflow 2.5.0

---

### 3. ml-service (Prediction Server)
**Purpose**: Load Production models and serve predictions

**Startup Flow**:
```
1. Read MLFLOW_TRACKING_URI from environment
2. Connect to MLflow server
3. Load models from Production stage:
   - waste-fill-time-model
   - waste-zone-generation-model
   - waste-route-score-model
4. Use loaded models for predictions
5. If MLflow unavailable → gracefully fallback to baseline heuristics
```

**REST Endpoints**:

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/health` | Service health + model version |
| GET | `/api/v1/ml/predict/fill-time` | Predict when bin will be full |
| GET | `/api/v1/ml/predict/zone-generation` | Forecast zone waste generation |
| POST | `/api/v1/ml/score/route` | Score route efficiency |
| GET | `/api/v1/ml/trends/waste-generation` | Get waste generation trends |
| **POST** | **`/internal/models/reload`** | **Reload models from MLflow** |

**Key Features**:
- Loads Production models at startup
- Graceful fallback to baseline if MLflow unavailable
- On-demand reload endpoint for Airflow notifications
- Optional: Log prediction metrics back to MLflow
- Request validation with Pydantic

**Environment Variables**:
```
MLFLOW_TRACKING_URI=http://mlflow:5000
MLFLOW_FILL_MODEL_NAME=waste-fill-time-model
MLFLOW_ZONE_MODEL_NAME=waste-zone-generation-model
MLFLOW_ROUTE_MODEL_NAME=waste-route-score-model
MLFLOW_MODEL_STAGE=Production
```

**Technology**: FastAPI, Pydantic, MLflow Python client

---

## Data Flow

### Training → Serving Flow

```
1. Airflow Scheduler triggers DAG daily
   ↓
2. Spark job trains models
   - Logs metrics: accuracy=0.94, val_acc=0.92
   - Saves model artifacts to file
   ↓
3. Airflow logs metrics to MLflow Experiment
   - Experiment: "waste-model-training"
   - Run: new run ID created
   ↓
4. Airflow registers model in MLflow Registry
   - Models:
     * waste-fill-time-model (v1, v2, v3...)
     * waste-zone-generation-model (v1, v2, v3...)
     * waste-route-score-model (v1, v2, v3...)
   ↓
5. Airflow promotes latest to Production stage
   - MLflow marks versions as:
     * Production: v3 (current)
     * Staging: v2 (candidate)
     * Archived: v1 (previous)
   ↓
6. Airflow calls ml-service POST /internal/models/reload
   - ml-service connects to MLflow
   - Loads Production models
   - Returns: {"status": "ok", "mlflow_enabled": true, ...}
   ↓
7. Airflow publishes Kafka event
   - Topic: waste.model.retrained
   - Downstream services notified of update
   ↓
8. ml-service now serves with new models
   - Prediction requests use loaded models
   - Falls back to baseline if model inference fails
```

---

## Deployment

### Docker Compose Stack

```yaml
Services:
├── mlflow (port 5000)
│   ├── Tracking Server
│   ├── Model Registry
│   └── Artifact Store (SQLite + local volume)
│
├── ml-service (port 8000)
│   ├── FastAPI server
│   ├── Uvicorn worker
│   └── Depends on: mlflow
│
├── airflow (port 8080)
│   ├── Webserver
│   ├── Scheduler
│   └── Depends on: postgres, mlflow
│
├── postgres
│   └── Airflow metadata database
│
├── kafka (port 9092)
│   └── Event streaming
│
└── zookeeper
    └── Kafka coordination
```

### Environment Variables

**MLflow**:
```
MLFLOW_BACKEND_STORE_URI=sqlite:///mlflow/mlflow.db
MLFLOW_DEFAULT_ARTIFACT_ROOT=/mlflow/artifacts
```

**ml-service**:
```
MLFLOW_TRACKING_URI=http://mlflow:5000
MLFLOW_FILL_MODEL_NAME=waste-fill-time-model
MLFLOW_ZONE_MODEL_NAME=waste-zone-generation-model
MLFLOW_ROUTE_MODEL_NAME=waste-route-score-model
MLFLOW_MODEL_STAGE=Production
```

**Airflow**:
```
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
MLFLOW_TRACKING_URI=http://mlflow:5000
ML_SERVICE_URL=http://ml-service:8000
```

---

## Running the Integration

### 1. Start All Services
```bash
docker-compose up -d
```

### 2. Wait for Services to Be Healthy
```bash
docker-compose ps
```

Monitor logs:
```bash
docker-compose logs -f mlflow
docker-compose logs -f ml-service
docker-compose logs -f airflow
```

### 3. Run Integration Tests
```bash
export MLFLOW_TRACKING_URI=http://localhost:5000
export ML_SERVICE_URL=http://localhost:8000

python tests/test_integration_pipeline.py
```

### 4. Manually Trigger Airflow DAG
```bash
# Access Airflow Web UI
# http://localhost:8080

# Or trigger via CLI
docker exec waste-airflow airflow dags trigger waste_spark_pipeline
```

### 5. Verify Model Loading
```bash
# Check ml-service health
curl http://localhost:8000/health

# Reload models manually
curl -X POST http://localhost:8000/internal/models/reload

# Make a prediction
curl "http://localhost:8000/api/v1/ml/predict/fill-time?current_fill_level=50"
```

### 6. Monitor MLflow
Visit: http://localhost:5000
- View experiments and runs
- Check model registry and versions
- Verify model stages

---

## Testing Strategy

### Unit Tests
- Test individual DAG tasks in isolation
- Mock external dependencies
- Validate model loading logic

### Integration Tests (`tests/test_integration_pipeline.py`)
- MLflow connectivity
- ml-service endpoints
- Airflow DAG structure
- End-to-end communication

### Manual Validation
- Trigger full pipeline in docker-compose
- Verify models appear in MLflow
- Check ml-service loads them
- Validate predictions work

---

## Key Design Decisions

1. **MLflow as Central Hub**: Single source of truth for models and experiments
2. **Stateless ml-service**: Can reload models without restarts
3. **Graceful Degradation**: Falls back to baseline if MLflow unavailable
4. **Event-Driven**: Kafka publishes updates for downstream services
5. **Container-Native**: All services run in Docker with health checks

---

## Troubleshooting

### ml-service can't connect to MLflow
```
Error: Cannot load models from MLflow
→ Check MLFLOW_TRACKING_URI environment variable
→ Ensure MLflow container is running: docker ps | grep mlflow
→ Test connectivity: curl http://mlflow:5000/health
```

### Airflow can't find ml-service
```
Error: Failed to notify ml-service
→ Check ML_SERVICE_URL environment variable
→ Verify ml-service is running: docker ps | grep ml-service
→ Test endpoint: curl http://ml-service:8000/health
```

### Models not showing in registry
```
Error: MLflow model registry is empty
→ Check if training job completed successfully
→ Verify models were registered in DAG logs
→ Check MLflow Web UI for experiments and runs
```

---

## Next Steps

1. **Production Models**: Replace placeholder metrics with real trained models
2. **Model Performance**: Add model validation and comparison before promotion
3. **Monitoring**: Add Prometheus/Grafana for pipeline monitoring
4. **CI/CD**: Integrate with GitHub Actions for automated deployment
5. **Multi-Stage**: Add Staging environment before Production
