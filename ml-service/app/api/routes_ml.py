from typing import Literal

from fastapi import APIRouter, Query

from app.schemas import (
    FillTimePredictionResponse,
    RoutePlanRequest,
    RouteScoreResponse,
    WasteGenerationTrendResponse,
    ZoneGenerationResponse,
)
from app.services.predictor import predictor

router = APIRouter(prefix="/api/v1/ml", tags=["ml"])


@router.get("/predict/fill-time", response_model=FillTimePredictionResponse)
def predict_fill_time(
    bin_id: str = Query(min_length=1),
    current_fill_level: float = Query(ge=0, le=100),
) -> FillTimePredictionResponse:
    predicted_full_at, confidence_interval = predictor.predict_fill_time(current_fill_level)
    return FillTimePredictionResponse(
        bin_id=bin_id,
        predicted_full_at=predicted_full_at,
        confidence_interval=confidence_interval,
        model_version=predictor.model_version,
    )


@router.get("/predict/zone-generation", response_model=ZoneGenerationResponse)
def predict_zone_generation(
    zone_id: int = Query(gt=0),
    date_range: str = Query(min_length=1),
) -> ZoneGenerationResponse:
    predicted_kg_per_day, by_waste_category = predictor.predict_zone_generation(zone_id, date_range)
    return ZoneGenerationResponse(
        zone_id=zone_id,
        date_range=date_range,
        predicted_kg_per_day=predicted_kg_per_day,
        by_waste_category=by_waste_category,
        model_version=predictor.model_version,
    )


@router.post("/score/route", response_model=RouteScoreResponse)
def score_route(payload: RoutePlanRequest) -> RouteScoreResponse:
    score, suggestions = predictor.score_route(
        vehicle_max_cargo_kg=payload.vehicle_max_cargo_kg,
        stop_weights=[stop.estimated_weight_kg for stop in payload.stops],
    )
    return RouteScoreResponse(
        efficiency_score=score,
        suggestions=suggestions,
        model_version=predictor.model_version,
    )


@router.get("/trends/waste-generation", response_model=WasteGenerationTrendResponse)
def trends_waste_generation(
    zone_id: int = Query(gt=0),
    period: Literal["week", "month", "quarter"] = Query(),
) -> WasteGenerationTrendResponse:
    return WasteGenerationTrendResponse(
        zone_id=zone_id,
        period=period,
        series=predictor.waste_generation_trends(zone_id, period),
        model_version=predictor.model_version,
    )
