from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class FillTimePredictionResponse(BaseModel):
    bin_id: str
    predicted_full_at: datetime
    confidence_interval: dict[str, float] = Field(
        description="Predicted hours-until-full confidence interval"
    )
    model_version: str


class ZoneGenerationResponse(BaseModel):
    zone_id: int
    date_range: str
    predicted_kg_per_day: float
    by_waste_category: dict[str, float]
    model_version: str


class RouteStop(BaseModel):
    bin_id: str
    estimated_weight_kg: float = Field(ge=0)


class RoutePlanRequest(BaseModel):
    zone_id: int
    route_type: Literal["routine", "emergency"]
    vehicle_max_cargo_kg: float = Field(gt=0)
    stops: list[RouteStop]


class RouteScoreResponse(BaseModel):
    efficiency_score: float = Field(ge=0, le=100)
    suggestions: list[str]
    model_version: str


class WasteTrendPoint(BaseModel):
    label: str
    food_waste: float
    paper: float
    glass: float
    plastic: float
    general: float
    e_waste: float


class WasteGenerationTrendResponse(BaseModel):
    zone_id: int
    period: Literal["week", "month", "quarter"]
    series: list[WasteTrendPoint]
    model_version: str


class HealthResponse(BaseModel):
    status: str
    service: str
    version: str
    model_version: str
    loaded_at: datetime
