import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from models import BinTelemetryEvent
from metadata_store import MetadataStore

logger = logging.getLogger(__name__)


class ValidationError(Exception):
    """Raised when event validation fails."""
    pass


class BinTelemetryProcessor:
    REQUIRED_FIELDS = {"bin_id", "fill_level_pct", "battery_level_pct", "timestamp"}
    LOW_BATTERY_THRESHOLD_PCT = 20.0
    WEAK_SIGNAL_THRESHOLD_DBM = -100.0
    MIN_VALID_TEMPERATURE_C = -20.0
    MAX_VALID_TEMPERATURE_C = 70.0
    WEIGHTS = {
        "fill_level": 0.40,
        "time_since_collection": 0.20,
        "predicted_fill": 0.20,
        "distance_cost": 0.10,
        "risk_factor": 0.10,
    }
    
    def __init__(self, metadata_store: MetadataStore) -> None:
        self.metadata_store = metadata_store

    @staticmethod
    def _classify_status(priority_score: float) -> str:
        if priority_score < 30:
            return "normal"
        if priority_score < 60:
            return "monitor"
        if priority_score < 85:
            return "urgent"
        return "critical"

    @staticmethod
    def _clamp_score(value: Any) -> float:
        if not isinstance(value, (int, float)):
            return 0.0
        return max(0.0, min(float(value), 100.0))

    @staticmethod
    def _parse_iso_datetime(value: Any) -> Optional[datetime]:
        if not isinstance(value, str):
            return None
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None

        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed

    @staticmethod
    def _extract_optional_numeric(payload: Dict[str, Any], metadata: Dict[str, Any], key: str) -> Optional[float]:
        for source in (payload, metadata):
            value = source.get(key)
            if isinstance(value, (int, float)):
                return float(value)
        return None

    @classmethod
    def _time_since_collection_score(cls, payload: Dict[str, Any], metadata: Dict[str, Any]) -> float:
        hours = cls._extract_optional_numeric(payload, metadata, "hours_since_last_collection")
        if hours is None:
            return 0.0
        return cls._clamp_score((hours / 24.0) * 100.0)

    @classmethod
    def _predicted_fill_score(
        cls,
        fill_level_pct: float,
        event_ts: datetime,
        payload: Dict[str, Any],
        metadata: Dict[str, Any],
    ) -> float:
        hours_until_full = cls._extract_optional_numeric(payload, metadata, "hours_until_full")
        if hours_until_full is None:
            predicted_full_at = payload.get("predicted_full_at") or metadata.get("predicted_full_at")
            predicted_full_dt = cls._parse_iso_datetime(predicted_full_at)
            if predicted_full_dt is not None:
                hours_until_full = (predicted_full_dt - event_ts).total_seconds() / 3600.0

        if hours_until_full is None:
            fill_rate = cls._extract_optional_numeric(payload, metadata, "fill_rate_pct_per_hour")
            if fill_rate is not None and fill_rate > 0:
                hours_until_full = (100.0 - fill_level_pct) / fill_rate

        if hours_until_full is None:
            return 0.0
        if hours_until_full <= 2:
            return 100.0
        if hours_until_full <= 6:
            return 80.0
        if hours_until_full <= 12:
            return 60.0
        if hours_until_full <= 24:
            return 40.0
        return 20.0

    @classmethod
    def _distance_cost_score(cls, payload: Dict[str, Any], metadata: Dict[str, Any]) -> float:
        distance_km = cls._extract_optional_numeric(payload, metadata, "distance_to_nearest_vehicle_km")
        if distance_km is None:
            return 0.0
        return cls._clamp_score((distance_km / 10.0) * 100.0)

    @classmethod
    def _risk_factor_score(cls, payload: Dict[str, Any], metadata: Dict[str, Any]) -> float:
        risk_factor = cls._extract_optional_numeric(payload, metadata, "risk_factor")
        if risk_factor is None:
            return 0.0
        return cls._clamp_score(risk_factor)

    @classmethod
    def _calculate_priority_score(
        cls,
        fill_level_pct: float,
        event_ts: datetime,
        payload: Dict[str, Any],
        metadata: Dict[str, Any],
    ) -> float:
        fill_level_score = cls._clamp_score(fill_level_pct)
        time_score = cls._time_since_collection_score(payload, metadata)
        predicted_score = cls._predicted_fill_score(fill_level_pct, event_ts, payload, metadata)
        distance_score = cls._distance_cost_score(payload, metadata)
        risk_score = cls._risk_factor_score(payload, metadata)

        score = (
            cls.WEIGHTS["fill_level"] * fill_level_score
            + cls.WEIGHTS["time_since_collection"] * time_score
            + cls.WEIGHTS["predicted_fill"] * predicted_score
            + cls.WEIGHTS["distance_cost"] * distance_score
            + cls.WEIGHTS["risk_factor"] * risk_score
        )
        return round(cls._clamp_score(score), 2)

    @staticmethod
    def _estimated_weight_kg(
        fill_level_pct: float,
        volume_litres: Optional[float],
        avg_kg_per_litre: Optional[float],
    ) -> Optional[float]:
        if volume_litres is None or avg_kg_per_litre is None:
            return None
        return float(fill_level_pct) * float(volume_litres) * float(avg_kg_per_litre)

    @classmethod
    def _detect_anomalies(cls, payload: Dict[str, Any]) -> Dict[str, Any]:
        flags = []

        battery_level_pct = payload.get("battery_level_pct")
        if isinstance(battery_level_pct, (int, float)) and battery_level_pct < cls.LOW_BATTERY_THRESHOLD_PCT:
            flags.append("low_battery")

        signal_strength_dbm = payload.get("signal_strength_dbm")
        if isinstance(signal_strength_dbm, (int, float)) and signal_strength_dbm < cls.WEAK_SIGNAL_THRESHOLD_DBM:
            flags.append("weak_signal")

        temperature_c = payload.get("temperature_c")
        if isinstance(temperature_c, (int, float)):
            if temperature_c < cls.MIN_VALID_TEMPERATURE_C or temperature_c > cls.MAX_VALID_TEMPERATURE_C:
                flags.append("abnormal_temperature")

        return {
            "anomaly_detected": len(flags) > 0,
            "anomaly_flags": flags,
        }

    def parse_kafka_event(self, raw_message: Dict[str, Any]) -> BinTelemetryEvent:
        """
        Parse and validate a raw Kafka message into a BinTelemetryEvent.
        
        Expected structure:
        {
          "payload": {
            "bin_id": "BIN-001",
            "fill_level_pct": 80,
            "battery_level_pct": 70,
            "timestamp": "2026-04-24T12:00:00Z",
            ...other fields...
          }
        }
        
        Args:
            raw_message: The raw message dict from Kafka
            
        Returns:
            BinTelemetryEvent if valid
            
        Raises:
            ValidationError: If validation fails
        """
        # Extract nested payload
        if not isinstance(raw_message, dict):
            raise ValidationError(f"Expected dict, got {type(raw_message)}")
        
        payload = raw_message.get("payload")
        if not payload:
            raise ValidationError("Missing 'payload' key in message")
        
        if not isinstance(payload, dict):
            raise ValidationError(f"'payload' must be dict, got {type(payload)}")
        
        # Validate required fields
        missing_fields = self.REQUIRED_FIELDS - set(payload.keys())
        if missing_fields:
            raise ValidationError(f"Missing required fields: {missing_fields}")
        
        # Extract and validate each field
        bin_id = payload.get("bin_id")
        if not isinstance(bin_id, str) or not bin_id.strip():
            raise ValidationError("bin_id must be non-empty string")
        
        fill_level = payload.get("fill_level_pct")
        if not isinstance(fill_level, (int, float)):
            raise ValidationError(f"fill_level_pct must be numeric, got {type(fill_level)}")
        if not (0 <= fill_level <= 100):
            raise ValidationError(f"fill_level_pct must be 0-100, got {fill_level}")
        
        battery_level = payload.get("battery_level_pct")
        if not isinstance(battery_level, (int, float)):
            raise ValidationError(f"battery_level_pct must be numeric, got {type(battery_level)}")
        if not (0 <= battery_level <= 100):
            raise ValidationError(f"battery_level_pct must be 0-100, got {battery_level}")
        
        timestamp_str = payload.get("timestamp")
        if not isinstance(timestamp_str, str):
            raise ValidationError(f"timestamp must be string, got {type(timestamp_str)}")
        
        try:
            # Support ISO format with or without 'Z' suffix
            timestamp_str_normalized = timestamp_str.replace("Z", "+00:00")
            event_ts = datetime.fromisoformat(timestamp_str_normalized)
        except ValueError as e:
            raise ValidationError(f"Invalid ISO timestamp format: {timestamp_str}") from e
        
        # Generate event ID from bin_id + timestamp
        event_id = f"{bin_id}_{event_ts.timestamp()}"
        
        return BinTelemetryEvent(
            event_id=event_id,
            event_ts=event_ts,
            payload=payload
        )

    def process(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            validated_event = self.parse_kafka_event(event)
            logger.debug(f"✓ Parsed event {validated_event.event_id}")

            bin_id = str(validated_event.payload["bin_id"])
            metadata = self.metadata_store.get_bin_metadata(bin_id)
            if metadata is None:
                logger.warning("✗ No metadata found for bin_id=%s", bin_id)
                return None

            fill_level_pct = float(validated_event.payload["fill_level_pct"])
            battery_level_pct = float(validated_event.payload["battery_level_pct"])
            volume_litres = metadata.get("volume_litres")
            avg_kg_per_litre = metadata.get("avg_kg_per_litre")
            priority_score = self._calculate_priority_score(
                fill_level_pct=fill_level_pct,
                event_ts=validated_event.event_ts,
                payload=validated_event.payload,
                metadata=metadata,
            )

            # e_waste special handling: bump urgency by +10 (spec §4 Pipeline 1)
            urgency_score = int(round(priority_score))
            if metadata.get("special_handling"):
                urgency_score = min(urgency_score + 10, 100)
            status = self._classify_status(float(urgency_score))

            processed = {
                "event_id": validated_event.event_id,
                "event_ts": validated_event.event_ts,
                "bin_id": bin_id,
                "cluster_id": metadata.get("cluster_id"),
                "zone_id": metadata.get("zone_id"),
                "latitude": metadata.get("latitude"),
                "longitude": metadata.get("longitude"),
                "waste_category_id": metadata.get("waste_category_id"),
                "waste_category": metadata.get("waste_category_name"),
                "fill_level_pct": fill_level_pct,
                "battery_level_pct": battery_level_pct,
                "volume_litres": volume_litres,
                "avg_kg_per_litre": avg_kg_per_litre,
                "estimated_weight_kg": self._estimated_weight_kg(
                    fill_level_pct=fill_level_pct,
                    volume_litres=volume_litres,
                    avg_kg_per_litre=avg_kg_per_litre,
                ),
                "status": status,
                "urgency_score": urgency_score,
                "priority_score": priority_score,
                "fill_rate_pct_per_hour": validated_event.payload.get("fill_rate_pct_per_hour"),
                "predicted_full_at": validated_event.payload.get("predicted_full_at"),
                "alerts": validated_event.payload.get("alerts", []),
                "timestamp": validated_event.payload.get("timestamp"),
            }

            processed.update(self._detect_anomalies(validated_event.payload))

            logger.debug("✓ Processed event %s for bin %s", validated_event.event_id, bin_id)
            return processed
        except ValidationError as e:
            logger.warning(f"✗ Validation error: {e}")
            return None
