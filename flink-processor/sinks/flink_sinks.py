"""
PyFlink SinkFunction wrappers for Pipeline 1.

Each class stores only the serializable Settings dataclass in __init__() and
creates real I/O clients in open(), which is the correct PyFlink pattern for
distributing work across task managers.
"""
import json
import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)


def _adapt_for_postgres(event: Dict[str, Any]) -> Dict[str, Any]:
    """Map the spec-format nested output to the flat dict PostgresSink.upsert_bin_current_state() expects."""
    payload = event.get("payload", {})
    return {
        "bin_id": payload.get("bin_id"),
        "fill_level_pct": payload.get("fill_level_pct"),
        "estimated_weight_kg": payload.get("estimated_weight_kg"),
        "status": payload.get("status"),
        "urgency_score": payload.get("urgency_score"),
        "predicted_full_at": payload.get("predicted_full_at"),
        "fill_rate_pct_per_hour": payload.get("fill_rate_pct_per_hour"),
        "battery_level_pct": payload.get("battery_level_pct"),
        "event_ts": event.get("timestamp"),
        "last_collected_at": None,
    }


def _adapt_for_influx_processed(event: Dict[str, Any]) -> Dict[str, Any]:
    """Map the spec-format nested output to the flat dict InfluxSink.write_processed_event() expects."""
    payload = event.get("payload", {})
    return {
        "bin_id": payload.get("bin_id"),
        "zone_id": payload.get("zone_id"),
        "waste_category": payload.get("waste_category"),
        "status": payload.get("status"),
        "fill_level_pct": payload.get("fill_level_pct"),
        "urgency_score": payload.get("urgency_score"),
        "estimated_weight_kg": payload.get("estimated_weight_kg"),
        "fill_rate_pct_per_hour": payload.get("fill_rate_pct_per_hour"),
        "predicted_full_at": payload.get("predicted_full_at"),
        "battery_level_pct": payload.get("battery_level_pct"),
        "event_ts": event.get("timestamp"),
    }


class ProcessedBinInfluxFlinkSink:
    """Writes the processed bin event to the InfluxDB processed bucket."""

    def __init__(self, settings):
        self._settings = settings
        self._influx = None

    def open(self, runtime_context):
        from sinks.influx_sink import InfluxSink
        self._influx = InfluxSink(self._settings)

    def invoke(self, value: str, context=None):
        try:
            event = json.loads(value) if isinstance(value, str) else value
            self._influx.write_processed_event(_adapt_for_influx_processed(event))
        except Exception:
            logger.exception("ProcessedBinInfluxFlinkSink.invoke failed")

    def close(self):
        if self._influx is not None:
            self._influx.close()


class BinStateFlinkSink:
    """Upserts the current bin state into PostgreSQL bin_current_state."""

    def __init__(self, settings):
        self._settings = settings
        self._pg = None

    def open(self, runtime_context):
        from sinks.postgres_sink import PostgresSink
        self._pg = PostgresSink(self._settings)

    def invoke(self, value: str, context=None):
        try:
            event = json.loads(value) if isinstance(value, str) else value
            self._pg.upsert_bin_current_state(_adapt_for_postgres(event))
        except Exception:
            logger.exception("BinStateFlinkSink.invoke failed")

    def close(self):
        if self._pg is not None:
            self._pg.close()


class KafkaProcessedFlinkSink:
    """Publishes the processed bin event to the waste.bin.processed Kafka topic."""

    def __init__(self, settings):
        self._settings = settings
        self._kafka = None

    def open(self, runtime_context):
        from sinks.kafka_sink import KafkaSink
        self._kafka = KafkaSink(self._settings)

    def invoke(self, value: str, context=None):
        try:
            event = json.loads(value) if isinstance(value, str) else value
            self._kafka.publish_processed(event)
        except Exception:
            logger.exception("KafkaProcessedFlinkSink.invoke failed")

    def close(self):
        if self._kafka is not None:
            self._kafka.close()
