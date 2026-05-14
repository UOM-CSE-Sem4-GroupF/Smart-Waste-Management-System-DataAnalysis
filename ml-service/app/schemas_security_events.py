"""
Pydantic models for security and event-based anomalies
Used for FastAPI request/response serialization
"""
from typing import Dict, List, Any, Optional
from datetime import datetime
from pydantic import BaseModel, Field


# Security Anomaly Models

class SecurityIncidentResponse(BaseModel):
    """Response model for security incident details"""
    id: int
    event_type: str = Field(..., description="COORDINATED_ATTACK, IMPOSSIBLE_PHYSICS, etc.")
    severity: str = Field(..., description="LOW, MEDIUM, HIGH, CRITICAL")
    attack_vector: Optional[str] = None
    affected_bins: List[str] = Field(default_factory=list)
    affected_zones: List[int] = Field(default_factory=list)
    source_ips: List[str] = Field(default_factory=list)
    suspicious_tokens: List[str] = Field(default_factory=list)
    evidence_json: Dict[str, Any] = Field(default_factory=dict)
    bayesian_probability: Optional[float] = Field(None, ge=0.0, le=1.0)
    resolution_status: str = Field(default="open", description="open, investigating, mitigated, confirmed, false_alarm")
    resolved_at: Optional[datetime] = None
    resolved_by: Optional[str] = None
    resolution_notes: Optional[str] = None
    timestamp: datetime
    created_at: datetime

    class Config:
        json_schema_extra = {
            "example": {
                "id": 1,
                "event_type": "COORDINATED_ATTACK",
                "severity": "CRITICAL",
                "attack_vector": "3+ critical failures across zones 1, 3, 5",
                "affected_bins": ["BIN-001", "BIN-042", "BIN-087"],
                "affected_zones": [1, 3, 5],
                "bayesian_probability": 0.92,
                "evidence_json": {
                    "critical_events_count": 3,
                    "time_window_minutes": 5
                },
                "timestamp": "2026-05-14T10:30:00Z",
                "created_at": "2026-05-14T10:30:01Z"
            }
        }


class SecurityDashboardSummary(BaseModel):
    """High-level security KPIs"""
    total_incidents_24h: int
    critical_incidents_24h: int
    open_incidents: int
    resolved_incidents_24h: int
    attack_types_distribution: Dict[str, int]
    avg_resolution_time_hours: Optional[float] = None
    false_alarm_rate_pct: Optional[float] = None
    last_incident_timestamp: Optional[datetime] = None


class NetworkActivityLog(BaseModel):
    """Network activity anomaly"""
    api_token_hash: str
    source_ip: str
    bin_id: Optional[str] = None
    reading_count: int
    request_timestamp: datetime
    anomaly_flags: List[str] = Field(default_factory=list)


class SecuritySearchFilters(BaseModel):
    """Request model for security incident search"""
    hours: int = Field(default=24, ge=1, le=720)
    limit: int = Field(default=100, ge=1, le=1000)
    event_type: Optional[str] = None
    severity: Optional[str] = None
    resolution_status: Optional[str] = None


class EntropyScoreResponse(BaseModel):
    """Entropy analysis result"""
    bin_id: str
    measurement_window_minutes: int
    fill_level_entropy: Optional[float]
    temperature_entropy: Optional[float]
    anomalous: bool
    flagged_reason: Optional[str]
    timestamp: datetime


# Event-Based Anomaly Models

class SpecialEventRequest(BaseModel):
    """Request model for logging a special event"""
    event_name: str
    zone_ids: List[int]
    start_time: datetime
    end_time: datetime
    event_type: str = Field(..., description="FESTIVAL, CONCERT, PROTEST, etc.")
    description: Optional[str] = None


class SpecialEventResponse(BaseModel):
    """Response model for special event"""
    id: int
    event_name: str
    zone_ids: List[int]
    start_time: datetime
    end_time: datetime
    event_type: str
    description: Optional[str] = None
    created_by: Optional[str] = None
    created_at: datetime

    class Config:
        json_schema_extra = {
            "example": {
                "id": 1,
                "event_name": "City Festival 2026",
                "zone_ids": [1, 2, 3],
                "start_time": "2026-05-14T09:00:00Z",
                "end_time": "2026-05-14T22:00:00Z",
                "event_type": "FESTIVAL",
                "description": "Annual city festival with heavy foot traffic",
                "created_by": "ops_admin",
                "created_at": "2026-05-14T08:00:00Z"
            }
        }


class EventImpactResponse(BaseModel):
    """Event impact analysis results"""
    event_id: int
    zone_id: int
    analysis_date: str
    anomalies_detected_count: int
    anomalies_high_severity_count: int
    anomalies_critical_count: int
    baseline_waste_kg: Optional[float] = None
    actual_waste_kg: Optional[float] = None
    waste_increase_pct: Optional[float] = None
    scheduled_collections_count: int
    executed_collections_count: int
    failed_collections_count: int
    collection_execution_rate: Optional[float] = None
    bins_with_chaotic_fill: int
    rapid_surge_incidents: int
    ml_prediction_accuracy: Optional[float] = None
    analysis_notes: Optional[str] = None


class AutoDetectedEventResponse(BaseModel):
    """Auto-detected event anomaly"""
    id: int
    zone_id: int
    detection_type: str = Field(..., description="CHAOTIC_DEMAND, RAPID_SURGE, FORECAST_DEVIATION")
    start_time: datetime
    end_time: datetime
    affected_bins_count: Optional[int] = None
    waste_volume_actual_kg: Optional[float] = None
    waste_volume_forecasted_kg: Optional[float] = None
    deviation_std_count: Optional[float] = None
    confidence_score: float = Field(..., ge=0.0, le=1.0)
    estimated_event_type: str
    estimated_event_probability: Dict[str, float]
    processed: bool
    linked_special_event_id: Optional[int] = None
    created_at: datetime

    class Config:
        json_schema_extra = {
            "example": {
                "id": 1,
                "zone_id": 2,
                "detection_type": "CHAOTIC_DEMAND",
                "start_time": "2026-05-14T14:00:00Z",
                "end_time": "2026-05-14T16:00:00Z",
                "waste_volume_actual_kg": 2450.5,
                "waste_volume_forecasted_kg": 1200.0,
                "deviation_std_count": 3.2,
                "confidence_score": 0.85,
                "estimated_event_type": "FESTIVAL",
                "estimated_event_probability": {
                    "FESTIVAL": 0.40,
                    "ACCIDENT": 0.30,
                    "CLEANING": 0.20,
                    "OTHER": 0.10
                },
                "processed": False,
                "created_at": "2026-05-14T16:05:00Z"
            }
        }


class EventListResponse(BaseModel):
    """List of special events"""
    events: List[SpecialEventResponse]
    total_count: int
    zone_id: Optional[int] = None
    days: int


# Anomaly Correlation Models

class AnomalyCorrelation(BaseModel):
    """Correlated anomalies for context"""
    anomaly_id: int
    anomaly_type: str
    severity: str
    timestamp: datetime
    bin_id: Optional[str] = None
    zone_id: Optional[int] = None


class AnomalyContextResponse(BaseModel):
    """Complete anomaly context with security and event correlation"""
    primary_anomaly: Dict[str, Any]
    related_security_incidents: List[SecurityIncidentResponse] = Field(default_factory=list)
    related_events: List[SpecialEventResponse] = Field(default_factory=list)
    auto_detected_events: List[AutoDetectedEventResponse] = Field(default_factory=list)
    recommended_actions: List[str] = Field(default_factory=list)
    
    class Config:
        json_schema_extra = {
            "example": {
                "primary_anomaly": {
                    "id": 100,
                    "anomaly_type": "data_quality",
                    "severity": "HIGH"
                },
                "related_security_incidents": [],
                "related_events": [
                    {
                        "id": 1,
                        "event_name": "City Festival",
                        "event_type": "FESTIVAL"
                    }
                ],
                "recommended_actions": [
                    "Correlate anomaly with Festival event (start: 09:00, end: 22:00)",
                    "No security incident detected",
                    "Normal pattern for such events"
                ]
            }
        }


# Unified Response Models

class PaginatedResponse(BaseModel):
    """Generic paginated response wrapper"""
    success: bool
    data: Any
    error: Optional[str] = None
    timestamp: datetime
    total_count: Optional[int] = None
    page: Optional[int] = None
    page_size: Optional[int] = None


class BulkSecurityEventResponse(BaseModel):
    """Bulk response for multiple security events"""
    success: bool
    data: List[SecurityIncidentResponse]
    error: Optional[str] = None
    timestamp: datetime
    total_count: int
