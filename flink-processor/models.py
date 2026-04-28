from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional


@dataclass
class BinTelemetryEvent:
    event_id: str
    event_ts: datetime
    payload: Dict[str, Any]


@dataclass
class EnrichedBinTelemetry:
    event: BinTelemetryEvent
    bin_id: str
    zone_id: Optional[int]
    latitude: Optional[float]
    longitude: Optional[float]
    volume_litres: Optional[float]
    waste_category_id: Optional[int]
    avg_kg_per_litre: Optional[float]
