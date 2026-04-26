from contextlib import asynccontextmanager
from fastapi import FastAPI
import logging

from app.api.routes_ml import router as ml_router
from app.core.config import SETTINGS
from app.schemas import HealthResponse
from app.services.predictor import predictor

@asynccontextmanager
async def lifespan(_: FastAPI):
    logging.getLogger("ml-service").info("Starting ML service and loading models.")
    predictor.load_models()
    yield


app = FastAPI(
    title="Smart Waste ML Service",
    version=SETTINGS.service_version,
    lifespan=lifespan,
)
app.include_router(ml_router)


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(
        status="ok",
        service=SETTINGS.service_name,
        version=SETTINGS.service_version,
        model_version=predictor.model_version,
        loaded_at=predictor.loaded_at,
    )
