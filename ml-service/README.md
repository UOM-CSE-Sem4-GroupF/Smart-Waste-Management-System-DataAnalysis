# ML Service (Service 5 - FastAPI)

This service exposes ML-related REST APIs for the Smart Waste Management System.

Current implementation provides a production-ready FastAPI skeleton with deterministic baseline logic. You can replace baseline methods with MLflow-loaded models without changing API contracts.

MLflow integration is now included. On startup, the service attempts to load production models from MLflow; if unavailable, it automatically falls back to baseline heuristic logic.

## Scope

- Folder ownership: `ml-service` only
- Language: Python 3.11
- Framework: FastAPI + Uvicorn
- Auth and gateway integration are handled upstream by Kong/Keycloak

## Implemented Endpoints

- `GET /api/v1/ml/predict/fill-time`
  - Query: `bin_id`, `current_fill_level`
  - Returns: `predicted_full_at`, `confidence_interval`, `model_version`

- `GET /api/v1/ml/predict/zone-generation`
  - Query: `zone_id`, `date_range`
  - Returns: `predicted_kg_per_day`, `by_waste_category`, `model_version`

- `POST /api/v1/ml/score/route`
  - Body: route plan with stops and estimated weights
  - Returns: `efficiency_score`, `suggestions`, `model_version`

- `GET /api/v1/ml/trends/waste-generation`
  - Query: `zone_id`, `period` (`week|month|quarter`)
  - Returns: time-series by waste category

- `GET /health`
  - Returns: `status`, `service`, `version`, `model_version`, `loaded_at`

## Local Run

From repository root:

```powershell
pip install -r ml-service/requirements.txt
uvicorn app.main:app --app-dir ml-service --reload --host 0.0.0.0 --port 8000
```

Open docs:

- `http://localhost:8000/docs`

## MLflow Configuration

Set these environment variables to enable startup model loading:

- `MLFLOW_TRACKING_URI` (required to enable loading)
- `MLFLOW_MODEL_STAGE` (default: `Production`)
- `MLFLOW_FILL_MODEL_NAME` (default: `waste-fill-time-model`)
- `MLFLOW_ZONE_MODEL_NAME` (default: `waste-zone-generation-model`)
- `MLFLOW_ROUTE_MODEL_NAME` (default: `waste-route-score-model`)

Example:

```powershell
$env:MLFLOW_TRACKING_URI = "http://mlflow:5000"
$env:MLFLOW_MODEL_STAGE = "Production"
uvicorn app.main:app --app-dir ml-service --reload --host 0.0.0.0 --port 8000
```

If `MLFLOW_TRACKING_URI` is not set, the service runs in baseline mode and still serves all APIs.

## Tests

```powershell
pytest ml-service/tests -q
```

## Docker

Build and run:

```powershell
docker build -t waste-ml-service ./ml-service
docker run --rm -p 8000:8000 waste-ml-service
```

## Project Structure

```text
ml-service/
  app/
    api/routes_ml.py
    core/config.py
    services/predictor.py
    main.py
    schemas.py
  tests/test_api.py
  requirements.txt
  Dockerfile
```

## Next Upgrade Path

1. Add MLflow client and model registry loading at startup.
2. Replace baseline methods in `app/services/predictor.py` with real model inference.
3. Add request tracing and structured JSON logging middleware.
4. Add contract tests for Kong-facing integration.