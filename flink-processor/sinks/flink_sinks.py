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

from pyflink.datastream.functions import FlatMapFunction, MapFunction, RuntimeContext

logger = logging.getLogger(__name__)


def _adapt_for_postgres(event: Dict[str, Any]) -> Dict[str, Any]:
    """Unpack nested spec payload into the flat dict PostgresSink.upsert_bin_current_state() expects."""
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
        "cluster_id": payload.get("cluster_id"),
        "zone_id": payload.get("zone_id"),
        "waste_category_id": payload.get("waste_category_id"),
        "volume_litres": payload.get("volume_litres"),
        "event_ts": event.get("timestamp"),  # top-level field in spec format
        "last_collected_at": None,
    }


def _adapt_for_influx_processed(event: Dict[str, Any]) -> Dict[str, Any]:
    """Unpack nested spec payload into the flat dict InfluxSink.write_processed_event() expects."""
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


class KafkaVehicleDeviationFlinkSink(MapFunction):
    """Publishes the vehicle deviation alert to the waste.vehicle.deviation topic and passes value through."""

    def __init__(self, settings):
        self._settings = settings
        self._kafka = None

    def open(self, runtime_context: RuntimeContext):
        from sinks.kafka_sink import KafkaSink
        self._kafka = KafkaSink(self._settings)

    def map(self, value: str) -> str:
        try:
            event = json.loads(value) if isinstance(value, str) else value
            self._kafka.publish_vehicle_deviation(event)
        except Exception:
            logger.exception("KafkaVehicleDeviationFlinkSink.map failed")
        return value

    def close(self):
        if self._kafka is not None:
            self._kafka.close()


class ZoneAggregationFlinkSink(FlatMapFunction):
    """Processes events through ZoneAggregationProcessor and sinks to Influx/Postgres/Kafka."""

    def __init__(self, settings):
        self._settings = settings
        self._processor = None
        self._influx = None
        self._pg = None
        self._kafka = None

    def open(self, runtime_context: RuntimeContext):
        from processors.zone_aggregation import ZoneAggregationProcessor
        from sinks.influx_sink import InfluxSink
        from sinks.postgres_sink import PostgresSink
        from sinks.kafka_sink import KafkaSink
        
        window_minutes = int(self._settings.zone_window_minutes or 10)
        slide_minutes = int(self._settings.zone_slide_minutes or 2)
        
        self._processor = ZoneAggregationProcessor(window_minutes=window_minutes, slide_minutes=slide_minutes)
        self._influx = InfluxSink(self._settings)
        self._pg = PostgresSink(self._settings)
        self._kafka = KafkaSink(self._settings)

    def flat_map(self, value: str):
        try:
            # Debug: show raw input
            print(f"DEBUG: Pipeline 2 received message: {str(value)[:100]}")
            
            raw_event = json.loads(value) if isinstance(value, str) else value
            # Unwrap the spec format if it exists
            payload = raw_event.get("payload", raw_event) if isinstance(raw_event, dict) else raw_event
            
            snapshots = self._processor.process(payload)
            if snapshots:
                print(f"DEBUG: Pipeline 2 generated {len(snapshots)} snapshots")
            
            for snapshot in snapshots:
                # 1. Write to Influx
                self._influx.write_zone_statistics(snapshot)
                # 2. Write to Postgres
                self._pg.insert_zone_snapshot(snapshot)

                # Wrap in standard spec format for Kafka and downstream consumers
                wrapped = {
                    "version": "1.0",
                    "source_service": "flink-pipeline-2",
                    "timestamp": snapshot.get("snapshot_at"),
                    "payload": snapshot
                }

                # 3. Publish to Kafka
                self._kafka.publish_zone_statistics(wrapped)
                
                print(f"DEBUG: Emitted Zone Snapshot: Zone={snapshot.get('zone_id')} Time={snapshot.get('snapshot_at')}")
                # 4. Emit for print/debug
                yield json.dumps(wrapped, default=str)
        except Exception as e:
            print(f"ERROR: ZoneAggregationFlinkSink failed: {str(e)}")
            logger.exception("ZoneAggregationFlinkSink failed")

    def close(self):
        if self._influx: self._influx.close()
        if self._pg: self._pg.close()
        if self._kafka: self._kafka.close()
