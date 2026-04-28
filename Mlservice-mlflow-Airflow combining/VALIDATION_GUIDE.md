# MLflow + ml-service + Airflow - Validation & Testing Guide

## 🚀 QUICK REFERENCE

**⏱️ Total Time: 10-15 minutes (mandatory tests only)**

```powershell
# 1. Start services (2 min)
docker-compose up -d
Start-Sleep -Seconds 60

# 2. Check services (1 min)
docker-compose ps

# 3. Test health endpoints (2 min)
curl -UseBasicParsing http://localhost:5000/health
curl -UseBasicParsing http://localhost:8000/health | ConvertFrom-Json

# 4. Test reload (1 min)
curl -UseBasicParsing -Method POST http://localhost:8000/internal/models/reload | ConvertFrom-Json

# 5. Test prediction (1 min)
curl -UseBasicParsing "http://localhost:8000/api/v1/ml/predict/fill-time?bin_id=1&current_fill_level=50" | ConvertFrom-Json

# 6. Check Airflow (1 min)
docker exec waste-airflow airflow dags list

# 7. Run tests (2 min) ✨ RECOMMENDED
$env:MLFLOW_TRACKING_URI="http://localhost:5000"
$env:ML_SERVICE_URL="http://localhost:8000"
python tests/test_integration_pipeline.py -v
```

**✅ If all 7 steps pass → Pipeline is ready!**

---

## 📋 DETAILED VALIDATION STEPS

### PHASE 1: Start the Stack (2 minutes)

**Step 1:** Start all services
```powershell
docker-compose up -d
```

**Step 2:** Wait 60 seconds, then verify all services are running
```powershell
docker-compose ps
```

**✅ Expected:** You should see 6 services - all with status `Up`:
- waste-airflow ✅
- waste-kafka ✅
- waste-ml-service ✅
- waste-mlflow ✅ (healthy)
- waste-postgres ✅ (healthy)
- waste-zookeeper ✅

---

### PHASE 2: Quick Health Checks (3 minutes)

**Step 3:** Test MLflow
```powershell
curl -UseBasicParsing http://localhost:5000/health
```
**✅ Expected:** Response code `200` with `OK`

**Step 4:** Test ml-service
```powershell
curl -UseBasicParsing http://localhost:8000/health | ConvertFrom-Json
```
**✅ Expected:** JSON response with:
```json
{
  "status": "ok",
  "service": "fastapi-ml-service",
  "version": "1.0.0"
}
```

**Step 5:** Test ml-service reload endpoint
```powershell
curl -UseBasicParsing -Method POST http://localhost:8000/internal/models/reload | ConvertFrom-Json
```
**✅ Expected:** JSON response with `"status": "ok"`

---

### PHASE 3: Test Predictions (2 minutes)

**Step 6:** Test a prediction endpoint (requires bin_id)
```powershell
curl -UseBasicParsing "http://localhost:8000/api/v1/ml/predict/fill-time?bin_id=1&current_fill_level=50" | ConvertFrom-Json
```
**✅ Expected:** JSON response with:
```json
{
  "bin_id": "1",
  "predicted_full_at": "2026-04-29T...",
  "confidence_interval": {
    "lower_hours": 10.0,
    "upper_hours": 15.0
  }
}
```

---

### PHASE 4: Verify Airflow DAG (2 minutes)

**Step 7:** Check Airflow has the DAG registered
```powershell
docker exec waste-airflow airflow dags list
```
**✅ Expected:** You should see:
```
dag_id               | filepath    | owner   | paused
=====================+=============+=========+=======
waste_spark_pipeline | main_dag.py | airflow | True
```

**Step 8:** (Optional) Access Airflow Web UI
```
Visit: http://localhost:8080
Username: admin
Password: admin
```
Look for the `waste_spark_pipeline` DAG.

---

### PHASE 5: Run Integration Tests (2 minutes) ⭐ RECOMMENDED

**Step 9:** Run automated test suite
```powershell
$env:MLFLOW_TRACKING_URI="http://localhost:5000"
$env:ML_SERVICE_URL="http://localhost:8000"
python tests/test_integration_pipeline.py -v
```
**✅ Expected:** All 10 tests pass:
```
Ran 10 tests in 0.243s
OK
Tests run: 10
Failures: 0
Errors: 0
```

---

### PHASE 6: (Optional) Trigger Training Pipeline (5-10 minutes)

**Step 10:** Unpause the DAG to allow scheduling
```powershell
docker exec waste-airflow airflow dags unpause waste_spark_pipeline
```

**Step 11:** Manually trigger Airflow DAG to train models
```powershell
docker exec waste-airflow airflow dags trigger waste_spark_pipeline
```
**✅ Expected:** Command returns without error

**Step 12:** Monitor DAG execution
```powershell
docker exec waste-airflow airflow dags list-runs --dag-id waste_spark_pipeline
```
**✅ Expected:** You see a new run with status `success` (after it completes)

**Step 13:** Check MLflow for experiments and models
```
http://localhost:5000
```
Navigate to:
- **Experiments** → Look for `waste-model-training`
- **Models** → Look for `waste-fill-time-model`, `waste-zone-generation-model`, `waste-route-score-model`

**✅ Expected:** You see 3 trained models with versions and stages

---

## ✅ MINIMUM SUCCESS CRITERIA (Phase 1-5 Only)

If all these pass, **your pipeline is working:**

- ✅ All 6 Docker services running (`docker-compose ps`)
- ✅ MLflow responds with 200 OK
- ✅ ml-service responds with health JSON
- ✅ ml-service reload endpoint works
- ✅ Prediction endpoint returns valid data
- ✅ Airflow DAG `waste_spark_pipeline` exists
- ✅ Integration tests pass (10/10)

---

## Full Integration Validation (OPTIONAL - 15-30 minutes)

This section provides advanced diagnostics and deep-dive testing. **Not required if Phases 1-5 pass.**

### Step 1: Verify Service Connectivity

**Test MLflow is reachable from ml-service**:
```bash
docker exec waste-ml-service python3 << 'EOF'
import mlflow
mlflow.set_tracking_uri("http://mlflow:5000")
experiments = mlflow.search_experiments()
print(f"✓ Connected to MLflow. Found {len(experiments)} experiments.")
EOF
```

**Test ml-service is reachable from Airflow**:
```bash
docker exec waste-airflow python3 << 'EOF'
from urllib.request import Request, urlopen
import json

url = "http://ml-service:8000/internal/models/reload"
req = Request(url, method="POST")
with urlopen(req, timeout=10) as response:
    data = json.loads(response.read().decode())
    print(f"✓ Connected to ml-service. Response: {data}")
EOF
```

### Step 2: Test Model Training Pipeline

**Manually trigger Airflow DAG**:
```bash
docker exec waste-airflow airflow dags trigger waste_spark_pipeline
```

**Monitor DAG execution**:
```bash
# Option 1: Via Web UI
# http://localhost:8080/dags/waste_spark_pipeline

# Option 2: Via CLI
docker exec waste-airflow airflow dags list-runs --dag-id waste_spark_pipeline
```

**Check DAG logs**:
```bash
docker exec waste-airflow airflow tasks log waste_spark_pipeline log_metrics_to_mlflow 2024-01-01
```

### Step 3: Verify Model Registration in MLflow

**Access MLflow Web UI**:
```
http://localhost:5000
```

Look for:
1. **Experiments** tab → `waste-model-training`
2. **Models** tab → Check for registered models:
   - waste-fill-time-model
   - waste-zone-generation-model
   - waste-route-score-model

3. Each model should show stages:
   - Production
   - Staging
   - Archived

### Step 4: Verify ml-service Loads Models

**Check ml-service logs**:
```bash
docker logs waste-ml-service
```

Look for messages like:
```
2026-04-28 ... INFO ... Loaded 3 MLflow model(s).
```

**Verify health endpoint shows MLflow enabled**:
```bash
curl http://localhost:8000/health | jq .mlflow_enabled

# Should return: true (if models loaded)
#            or: false (if MLflow unavailable)
```

### Step 5: Test Model Predictions

**Test with loaded model**:
```bash
curl "http://localhost:8000/api/v1/ml/predict/fill-time?current_fill_level=75" | jq

# Should return prediction using loaded model
```

**Test other endpoints**:
```bash
# Zone generation forecast
curl "http://localhost:8000/api/v1/ml/predict/zone-generation?zone_id=1&date_range=week" | jq

# Route scoring
curl -X POST http://localhost:8000/api/v1/ml/score/route \
  -H "Content-Type: application/json" \
  -d '{"vehicle_max_cargo_kg": 5000, "stop_weights": [100, 200, 150]}' | jq

# Waste trends
curl "http://localhost:8000/api/v1/ml/trends/waste-generation?zone_id=1&period=week" | jq
```

---

## Troubleshooting Commands

## Data Flow Validation Checklist

### ✓ Data Flow: Training → Serving

- [ ] **Spark trains model**
  ```bash
  docker logs $(docker ps -q -f "label=service=spark") 2>/dev/null | grep -i "training\|complete"
  ```

- [ ] **Airflow logs metrics to MLflow**
  - Check MLflow Web UI → Experiments → waste-model-training
  - Should show params: `model_type`, `training_date`
  - Should show metrics: `training_accuracy`, `validation_accuracy`, `model_size_mb`

- [ ] **Models registered in MLflow**
  - Visit MLflow Web UI → Models
  - Should show 3 models with version numbers

- [ ] **Models promoted to Production**
  - In MLflow UI, click on each model
  - Verify "Production" stage exists with latest version

- [ ] **ml-service reloads models**
  - Check ml-service logs for success message
  - Check health endpoint: `mlflow_enabled` should be `true`

- [ ] **Predictions work with loaded models**
  - Call any prediction endpoint
  - Verify response contains valid data

- [ ] **Kafka event published**
  - Check Kafka topic `waste.model.retrained`
  ```bash
  docker exec waste-kafka kafka-console-consumer \
    --bootstrap-servers localhost:9092 \
    --topic waste.model.retrained \
    --from-beginning
  ```

---

## Troubleshooting Commands

### Check Service Logs
```bash
# MLflow
docker logs waste-mlflow -f

# ml-service
docker logs waste-ml-service -f

# Airflow
docker logs waste-airflow -f

# All services
docker-compose logs -f
```

### Verify Network Communication
```bash
# From ml-service to MLflow
docker exec waste-ml-service curl http://mlflow:5000/health

# From Airflow to ml-service
docker exec waste-airflow curl http://ml-service:8000/health

# From host machine
curl http://localhost:5000/health
curl http://localhost:8000/health
```

### Inspect MLflow Database
```bash
# List all experiments
docker exec waste-mlflow sqlite3 /mlflow/mlflow.db "SELECT * FROM experiments;"

# List all runs
docker exec waste-mlflow sqlite3 /mlflow/mlflow.db "SELECT * FROM runs LIMIT 10;"

# List registered models
docker exec waste-mlflow sqlite3 /mlflow/mlflow.db "SELECT * FROM registered_models;"
```

### Test Model Loading (Inside ml-service)
```bash
docker exec waste-ml-service python3 << 'EOF'
import os
from app.services.predictor import predictor

print(f"MLflow URI: {os.getenv('MLFLOW_TRACKING_URI')}")
print(f"MLflow Enabled: {predictor.mlflow_enabled}")
print(f"Model Version: {predictor.model_version}")
print(f"Loaded Models: {list(predictor._models.keys())}")
EOF
```

---

## Performance Validation

### Response Time Benchmarks

**Health Check** (should be <10ms):
```bash
time curl http://localhost:8000/health
```

**Prediction Endpoint** (should be <50ms):
```bash
time curl "http://localhost:8000/api/v1/ml/predict/fill-time?current_fill_level=50"
```

**Reload Endpoint** (should be <1000ms):
```bash
time curl -X POST http://localhost:8000/internal/models/reload
```

### Load Testing (Optional)
```bash
# Using Apache Bench
ab -n 100 -c 10 http://localhost:8000/health

# Using curl loop
for i in {1..100}; do curl -s http://localhost:8000/health > /dev/null; done
```

---

## Success Criteria

### ✅ MANDATORY (Phases 1-5 must all pass)

- [ ] All Docker containers up (`docker-compose ps`)
- [ ] MLflow server responds (`curl http://localhost:5000/health`)
- [ ] ml-service responds (`curl http://localhost:8000/health`)
- [ ] ml-service reload endpoint works (`curl -X POST http://localhost:8000/internal/models/reload`)
- [ ] Predictions return valid data (`curl http://localhost:8000/api/v1/ml/predict/fill-time?bin_id=1&current_fill_level=50`)
- [ ] Airflow DAG registered (`docker exec waste-airflow airflow dags list`)
- [ ] Integration tests pass (10/10) ✨

### 🔧 OPTIONAL (Advanced validation - Phase 6)

- [ ] Airflow Web UI loads (http://localhost:8080)
- [ ] MLflow Web UI loads (http://localhost:5000)
- [ ] Airflow DAG triggers successfully
- [ ] MLflow shows `waste-model-training` experiment
- [ ] MLflow shows 3 registered models
- [ ] Models promoted to Production stage
- [ ] ml-service loads models from MLflow (mlflow_enabled: true)
- [ ] Kafka events are published

---

## Clean Up

### Stop all services
```bash
docker-compose down
```

### Remove all volumes (careful!)
```bash
docker-compose down -v
```

### View disk usage
```bash
docker system df
```

---

## Next Steps

Once validation is complete:

1. **Integrate Real Models**: Replace baseline with actual trained models
2. **Set Up Monitoring**: Add Prometheus + Grafana
3. **Add Model Validation**: Implement model comparison and validation gates
4. **Scale ml-service**: Add load balancing and replicas
5. **Automate Deployment**: Set up CI/CD pipeline
