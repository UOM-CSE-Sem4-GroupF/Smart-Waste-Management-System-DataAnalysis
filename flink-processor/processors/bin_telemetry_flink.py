import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from pyflink.datastream.functions import KeyedProcessFunction

logger = logging.getLogger(__name__)

COOLDOWN_MS = 30 * 60 * 1000  # 30 minutes


def _parse_timestamp_ms(timestamp_str: str) -> Optional[int]:
    try:
        dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    except (ValueError, AttributeError):
        return None


def _ms_to_iso(epoch_ms: Optional[int]) -> Optional[str]:
    if epoch_ms is None:
        return None
    dt = datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def classify_urgency(fill_level: float, fill_rate: Optional[float]):
    """
    Returns (status, urgency_score).
    Implements the exact scoring from spec §4.2.
    """
    if fill_level >= 90:
        base_score = 85 + int((fill_level - 90) * 1.5)
        status = "critical"
    elif fill_level >= 75:
        base_score = 60 + int((fill_level - 75) * 1.67)
        status = "urgent"
    elif fill_level >= 50:
        base_score = 30 + int((fill_level - 50) * 1.2)
        status = "monitor"
    else:
        base_score = int(fill_level * 0.6)
        status = "normal"

    score = base_score
    if fill_rate is not None:
        if fill_rate > 15:
            score += 15
        elif fill_rate > 10:
            score += 10
        elif fill_rate > 5:
            score += 5

    score = min(score, 100)

    if score >= 85:
        status = "critical"
    elif score >= 60:
        status = "urgent"
    elif score >= 30:
        status = "monitor"
    else:
        status = "normal"

    return status, score


def detect_anomaly(
    fill_level: float,
    fill_rate: Optional[float],
    battery: float,
    event_ms: int,
    last_alert_ms: Optional[int],
) -> Optional[Dict[str, Any]]:
    """
    Returns anomaly dict or None.
    Spec §4.2: throttle to one alert per bin per 30 minutes.
    """
    if last_alert_ms is not None and (event_ms - last_alert_ms) < COOLDOWN_MS:
        return None

    if fill_rate is not None:
        if fill_rate > 15:
            return {"type": "RAPID_FILLING", "fill_rate": fill_rate}
        if fill_rate < -5:
            return {"type": "POSSIBLE_TAMPERING", "fill_rate": fill_rate}

    if battery < 10:
        return {"type": "LOW_BATTERY", "battery_pct": battery}

    return None


class BinTelemetryFlinkProcessor(KeyedProcessFunction):
    """
    PyFlink KeyedProcessFunction for stateful bin telemetry processing (Pipeline 1).

    Keyed by bin_id — Flink guarantees all events for the same bin_id reach the
    same instance, so state (prev fill level, prev timestamp, last alert time) is
    consistent per bin without external coordination.

    Usage in a PyFlink job:
        stream.key_by(lambda s: json.loads(s)["payload"]["bin_id"])
              .process(BinTelemetryFlinkProcessor(), output_type=Types.STRING())

    Inputs:  JSON-encoded raw telemetry strings (from Kafka source)
    Outputs: JSON-encoded processed event strings (nested spec format)
    """

    def __init__(self):
        self._prev_level = None
        self._prev_ts = None
        self._last_alert_ts = None
        self._metadata_store = None
        self._influx_sink = None  # for raw write before yielding

    # ------------------------------------------------------------------
    # PyFlink lifecycle hooks
    # ------------------------------------------------------------------

    def open(self, runtime_context):
        from config import load_settings
        from metadata_store import MetadataStore
        from sinks.influx_sink import InfluxSink
        from pyflink.datastream.state import ValueStateDescriptor
        from pyflink.common import Types

        settings = load_settings()
        self._metadata_store = MetadataStore(settings)
        self._influx_sink = InfluxSink(settings)

        self._prev_level = runtime_context.get_state(
            ValueStateDescriptor("prev_level", Types.FLOAT())
        )
        self._prev_ts = runtime_context.get_state(
            ValueStateDescriptor("prev_ts", Types.LONG())
        )
        self._last_alert_ts = runtime_context.get_state(
            ValueStateDescriptor("last_alert_ts", Types.LONG())
        )

    def close(self):
        if self._metadata_store is not None:
            self._metadata_store.close()
        if self._influx_sink is not None:
            self._influx_sink.close()

    def process_element(self, event_json: str, ctx):
        try:
            yield from self._handle(event_json)
        except Exception:
            logger.exception("Error processing event: %.200s", event_json)

    # ------------------------------------------------------------------
    # Core processing
    # ------------------------------------------------------------------

    def _handle(self, event_json: str):
        raw_event = json.loads(event_json) if isinstance(event_json, str) else event_json
        payload = raw_event.get("payload") if isinstance(raw_event, dict) else None
        if not payload or not isinstance(payload, dict):
            logger.warning("Skipping event: missing or invalid payload")
            return

        bin_id = payload.get("bin_id")
        fill_level = payload.get("fill_level_pct")
        battery = payload.get("battery_level_pct")
        timestamp_str = payload.get("timestamp")

        if not all([bin_id, fill_level is not None, battery is not None, timestamp_str]):
            logger.warning("Skipping event: missing required fields for bin_id=%s", bin_id)
            return

        fill_level = float(fill_level)
        battery = float(battery)
        event_ms = _parse_timestamp_ms(timestamp_str)
        if event_ms is None:
            logger.warning("Skipping event: unparseable timestamp '%s'", timestamp_str)
            return

        # Write raw reading to InfluxDB immediately (spec §4.1)
        if self._influx_sink is not None:
            try:
                self._influx_sink.write_raw_event(raw_event)
            except Exception:
                logger.warning("Raw InfluxDB write failed for bin=%s", bin_id)

        # Load bin metadata
        metadata = self._metadata_store.get_bin_metadata(bin_id) if self._metadata_store else None
        if metadata is None:
            logger.warning("Unknown bin skipped: %s", bin_id)
            return

        # Estimated weight
        volume = metadata.get("volume_litres")
        density = metadata.get("avg_kg_per_litre")
        estimated_weight_kg = None
        if volume is not None and density is not None:
            estimated_weight_kg = round((fill_level / 100.0) * float(volume) * float(density), 2)

        # Fill rate from Flink state
        fill_rate: Optional[float] = None
        predicted_full_at: Optional[str] = None

        prev_level = self._prev_level.value() if self._prev_level else None
        prev_ts = self._prev_ts.value() if self._prev_ts else None

        if prev_level is not None and prev_ts is not None:
            time_diff_hours = (event_ms - prev_ts) / 3_600_000
            if time_diff_hours > 0:
                fill_rate = round((fill_level - prev_level) / time_diff_hours, 3)
                if fill_rate is not None and fill_rate > 0:
                    hours_until_full = (100.0 - fill_level) / fill_rate
                    predicted_full_ms = event_ms + int(hours_until_full * 3_600_000)
                    predicted_full_at = _ms_to_iso(predicted_full_ms)

        # Update state
        if self._prev_level:
            self._prev_level.update(fill_level)
        if self._prev_ts:
            self._prev_ts.update(event_ms)

        # Urgency and anomaly
        status, urgency_score = classify_urgency(fill_level, fill_rate)

        # Fix numeric overflow for NUMERIC(6,3) column (max value 999.999)
        if fill_rate is not None:
            fill_rate = max(-999.999, min(fill_rate, 999.999))

        last_alert_ms = self._last_alert_ts.value() if self._last_alert_ts else None
        anomaly = detect_anomaly(fill_level, fill_rate, battery, event_ms, last_alert_ms)
        if anomaly and self._last_alert_ts:
            self._last_alert_ts.update(event_ms)

        processed = {
            "event_id": f"{bin_id}_{event_ms}",
            "event_ts": timestamp_str,
            "bin_id": bin_id,
            "cluster_id": metadata.get("cluster_id"),
            "zone_id": metadata.get("zone_id"),
            "latitude": metadata.get("latitude"),
            "longitude": metadata.get("longitude"),
            "waste_category_id": metadata.get("waste_category_id"),
            "waste_category": metadata.get("waste_category_name"),
            "fill_level_pct": fill_level,
            "battery_level_pct": battery,
            "estimated_weight_kg": estimated_weight_kg,
            "status": status,
            "urgency_score": urgency_score,
            "fill_rate_pct_per_hour": fill_rate,
            "predicted_full_at": predicted_full_at,
            "volume_litres": volume if volume is not None else 0.0,
            "alerts": [anomaly["type"]] if anomaly else [],
            "timestamp": timestamp_str,
            "anomaly_detected": bool(anomaly),
            "anomaly_flags": [anomaly] if anomaly else [],
        }

        yield json.dumps(processed, default=str)
