import json
from datetime import datetime
from typing import Any, Dict, Optional


class ValidationError(Exception):
    """Raised when vehicle GPS validation fails."""


class VehiclePositionProcessor:
    REQUIRED_FIELDS = {"vehicle_id", "latitude", "longitude", "timestamp"}

    @staticmethod
    def _parse_timestamp(value: Any) -> Optional[datetime]:
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                return None
        return None

    @classmethod
    def parse_kafka_message(cls, raw_message: Any) -> Dict[str, Any]:
        if isinstance(raw_message, (bytes, bytearray)):
            try:
                raw_message = raw_message.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise ValidationError("Malformed JSON") from exc

        if isinstance(raw_message, str):
            try:
                raw_message = json.loads(raw_message)
            except json.JSONDecodeError as exc:
                raise ValidationError("Malformed JSON") from exc

        if not isinstance(raw_message, dict):
            raise ValidationError(f"Expected JSON object, got {type(raw_message)}")

        missing_fields = cls.REQUIRED_FIELDS - set(raw_message.keys())
        if missing_fields:
            raise ValidationError(f"Missing required fields: {sorted(missing_fields)}")

        vehicle_id = raw_message.get("vehicle_id")
        if not isinstance(vehicle_id, str) or not vehicle_id.strip():
            raise ValidationError("vehicle_id must be a non-empty string")

        latitude = raw_message.get("latitude")
        if not isinstance(latitude, (int, float)):
            raise ValidationError("latitude must be numeric")
        if latitude < -90 or latitude > 90:
            raise ValidationError("latitude must be between -90 and 90")

        longitude = raw_message.get("longitude")
        if not isinstance(longitude, (int, float)):
            raise ValidationError("longitude must be numeric")
        if longitude < -180 or longitude > 180:
            raise ValidationError("longitude must be between -180 and 180")

        timestamp = raw_message.get("timestamp")
        if not isinstance(timestamp, str):
            raise ValidationError("timestamp must be a non-empty string")

        parsed_timestamp = cls._parse_timestamp(timestamp)
        if parsed_timestamp is None:
            raise ValidationError("timestamp must be a valid ISO-8601 value")

        return {
            "vehicle_id": vehicle_id,
            "latitude": float(latitude),
            "longitude": float(longitude),
            "timestamp": parsed_timestamp,
            "job_id": raw_message.get("job_id"),
        }

    def process(self, raw_message: Any) -> Dict[str, Any]:
        return self.parse_kafka_message(raw_message)