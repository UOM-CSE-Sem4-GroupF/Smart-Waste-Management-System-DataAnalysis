import json
import logging
from pyflink.datastream.functions import FlatMapFunction, RuntimeContext

logger = logging.getLogger(__name__)

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
            event = json.loads(value) if isinstance(value, str) else value
            snapshots = self._processor.process(event)
            for snapshot in snapshots:
                # 1. Write to Influx
                self._influx.write_zone_statistics(snapshot)
                # 2. Write to Postgres
                self._pg.upsert_zone_statistics(snapshot)
                # 3. Publish to Kafka
                self._kafka.publish_zone_statistics(snapshot)
                # 4. Emit for print/debug
                yield json.dumps(snapshot, default=str)
        except Exception:
            logger.exception("ZoneAggregationFlinkSink failed")

    def close(self):
        if self._influx: self._influx.close()
        if self._pg: self._pg.close()
        if self._kafka: self._kafka.close()
