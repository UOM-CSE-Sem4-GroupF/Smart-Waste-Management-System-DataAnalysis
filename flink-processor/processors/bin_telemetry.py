import logging
from datetime import datetime
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
    
    def __init__(self, metadata_store: MetadataStore) -> None:
        self.metadata_store = metadata_store

    @staticmethod
    def _classify_status(fill_level_pct: float) -> str:
        if fill_level_pct < 50:
            return "normal"
        if fill_level_pct < 75:
            return "monitor"
        if fill_level_pct <= 90:
            return "urgent"
        return "critical"

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

        signal_strength = payload.get("signal_strength")
        if isinstance(signal_strength, (int, float)) and signal_strength < cls.WEAK_SIGNAL_THRESHOLD_DBM:
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

            processed = {
                "event_id": validated_event.event_id,
                "event_ts": validated_event.event_ts,
                "bin_id": bin_id,
                "zone_id": metadata.get("zone_id"),
                "latitude": metadata.get("latitude"),
                "longitude": metadata.get("longitude"),
                "waste_category_id": metadata.get("waste_category_id"),
                "fill_level_pct": fill_level_pct,
                "battery_level_pct": battery_level_pct,
                "volume_litres": volume_litres,
                "avg_kg_per_litre": avg_kg_per_litre,
                "estimated_weight_kg": self._estimated_weight_kg(
                    fill_level_pct=fill_level_pct,
                    volume_litres=volume_litres,
                    avg_kg_per_litre=avg_kg_per_litre,
                ),
                "status": self._classify_status(fill_level_pct),
                "urgency_score": int(round(fill_level_pct)),
            }

            processed.update(self._detect_anomalies(validated_event.payload))

            logger.debug("✓ Processed event %s for bin %s", validated_event.event_id, bin_id)
            return processed
        except ValidationError as e:
            logger.warning(f"✗ Validation error: {e}")
            return None
