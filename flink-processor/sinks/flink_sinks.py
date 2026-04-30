"""
PyFlink MapFunction wrappers for Pipeline 1 sinks.

PyFlink's add_sink() requires a Java-backed SinkFunction. For pure Python I/O
(InfluxDB, Postgres, kafka-python), the correct pattern is to use .map() as a
"fire-and-forget" operation — the MapFunction performs the write and returns the
unchanged value, which is passed to the next stage.

Each class creates real I/O clients lazily in open(), called once per task-manager
instance, following the standard PyFlink serialization pattern.

The processor outputs the nested spec format:
  {"version": "1.0", "source_service": "...", "timestamp": "...", "payload": {...}}

The adapters below unpack "payload" before passing to the existing sink methods.
"""
import json
import logging
from typing import Any, Dict

from pyflink.datastream.functions import MapFunction, RuntimeContext

logger = logging.getLogger(__name__)


def _adapt_for_postgres(event: Dict[str, Any]) -> Dict[str, Any]:
    """Unpack nested spec payload or handle flat dict for PostgresSink."""
    payload = event.get("payload")
    # Fallback to the event itself if it's already flat (Flink processor output)
    data = payload if isinstance(payload, dict) else event
    
    return {
        "bin_id": data.get("bin_id"),
        "fill_level_pct": data.get("fill_level_pct"),
        "estimated_weight_kg": data.get("estimated_weight_kg"),
        "status": data.get("status"),
        "urgency_score": data.get("urgency_score"),
        "predicted_full_at": data.get("predicted_full_at"),
        "fill_rate_pct_per_hour": data.get("fill_rate_pct_per_hour"),
        "battery_level_pct": data.get("battery_level_pct"),
        "cluster_id": data.get("cluster_id"),
        "zone_id": data.get("zone_id"),
        "waste_category_id": data.get("waste_category_id"),
        "volume_litres": data.get("volume_litres"),
        "event_ts": event.get("timestamp") or data.get("event_ts"),
        "last_reading_at": event.get("timestamp") or data.get("event_ts"),
    }


def _adapt_for_influx_processed(event: Dict[str, Any]) -> Dict[str, Any]:
    """Unpack nested spec payload or handle flat dict for InfluxSink."""
    payload = event.get("payload")
    data = payload if isinstance(payload, dict) else event
    
    return {
        "bin_id": data.get("bin_id"),
        "zone_id": data.get("zone_id"),
        "waste_category": data.get("waste_category"),
        "status": data.get("status"),
        "fill_level_pct": data.get("fill_level_pct"),
        "urgency_score": data.get("urgency_score"),
        "estimated_weight_kg": data.get("estimated_weight_kg"),
        "fill_rate_pct_per_hour": data.get("fill_rate_pct_per_hour"),
        "predicted_full_at": data.get("predicted_full_at"),
        "battery_level_pct": data.get("battery_level_pct"),
        "event_ts": event.get("timestamp") or data.get("event_ts"),
    }


class ProcessedBinInfluxFlinkSink(MapFunction):
    """Writes the processed bin event to InfluxDB and passes the value through unchanged."""

    def __init__(self, settings):
        self._settings = settings
        self._influx = None

    def open(self, runtime_context: RuntimeContext):
        from sinks.influx_sink import InfluxSink
        self._influx = InfluxSink(self._settings)

    def map(self, value: str) -> str:
        try:
            event = json.loads(value) if isinstance(value, str) else value
            self._influx.write_processed_event(_adapt_for_influx_processed(event))
        except Exception:
            logger.exception("ProcessedBinInfluxFlinkSink.map failed")
        return value

    def close(self):
        if self._influx is not None:
            self._influx.close()


class BinStateFlinkSink(MapFunction):
    """Upserts the current bin state into PostgreSQL bin_current_state and passes value through."""

    def __init__(self, settings):
        self._settings = settings
        self._pg = None

    def open(self, runtime_context: RuntimeContext):
        from sinks.postgres_sink import PostgresSink
        self._pg = PostgresSink(self._settings)

    def map(self, value: str) -> str:
        try:
            event = json.loads(value) if isinstance(value, str) else value
            self._pg.upsert_bin_current_state(_adapt_for_postgres(event))
        except Exception:
            logger.exception("BinStateFlinkSink.map failed")
        return value

    def close(self):
        if self._pg is not None:
            self._pg.close()


class KafkaProcessedFlinkSink(MapFunction):
    """Publishes the processed bin event to the waste.bin.processed topic and passes value through."""

    def __init__(self, settings):
        self._settings = settings
        self._kafka = None

    def open(self, runtime_context: RuntimeContext):
        from sinks.kafka_sink import KafkaSink
        self._kafka = KafkaSink(self._settings)

    def map(self, value: str) -> str:
        try:
            event = json.loads(value) if isinstance(value, str) else value
            self._kafka.publish_processed(event)
        except Exception:
            logger.exception("KafkaProcessedFlinkSink.map failed")
        return value

    def close(self):
        if self._kafka is not None:
            self._kafka.close()
