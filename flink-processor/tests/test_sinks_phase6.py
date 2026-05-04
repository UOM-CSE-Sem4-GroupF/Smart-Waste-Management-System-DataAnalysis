from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from config import load_settings
from sinks.influx_sink import InfluxSink
from sinks.kafka_sink import KafkaSink
from sinks.postgres_sink import PostgresSink


def test_kafka_sink_publish_processed():
    settings = load_settings()
    with patch("sinks.kafka_sink.KafkaProducer") as mock_producer_cls:
        producer = MagicMock()
        future = MagicMock()
        future.get.return_value = MagicMock(partition=0, offset=5)
        producer.send.return_value = future
        mock_producer_cls.return_value = producer

        sink = KafkaSink(settings)
        event = {"bin_id": "BIN-001", "status": "urgent"}

        sink.publish_processed(event)

        producer.send.assert_called_once_with(settings.kafka_output_topic, event)
        future.get.assert_called_once_with(timeout=10)


def test_postgres_sink_upsert_bin_current_state():
    settings = load_settings()
    with patch("sinks.postgres_sink.pool.SimpleConnectionPool") as mock_pool_cls:
        mock_pool = MagicMock()
        mock_pool_cls.return_value = mock_pool

        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cur
        mock_pool.getconn.return_value = conn

        sink = PostgresSink(settings)
        record = {
            "bin_id": "BIN-001",
            "fill_level_pct": 82.5,
            "estimated_weight_kg": 45.2,
            "status": "urgent",
            "urgency_score": 78,
            "battery_level_pct": 67.0,
            "event_ts": datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc),
        }

        sink.upsert_bin_current_state(record)

        assert cur.execute.called
        conn.commit.assert_called_once()
        mock_pool.putconn.assert_called_once_with(conn)


def test_influx_sink_write_processed_event():
    settings = load_settings()
    with patch("sinks.influx_sink.InfluxDBClient") as mock_client_cls, patch(
        "sinks.influx_sink.Point"
    ) as mock_point_cls:
        mock_client = MagicMock()
        mock_write_api = MagicMock()
        mock_client.write_api.return_value = mock_write_api
        mock_client_cls.return_value = mock_client

        point = MagicMock()
        point.tag.return_value = point
        point.field.return_value = point
        point.time.return_value = point
        mock_point_cls.return_value = point

        sink = InfluxSink(settings)
        event = {
            "event_ts": "2026-04-24T12:00:00Z",
            "bin_id": "BIN-001",
            "zone_id": 1,
            "waste_category_id": 2,
            "status": "urgent",
            "fill_level_pct": 82.5,
            "urgency_score": 78,
            "estimated_weight_kg": 45.2,
        }

        sink.write_processed_event(event)

        mock_write_api.write.assert_called_once()


def test_influx_sink_write_raw_event():
    settings = load_settings()
    with patch("sinks.influx_sink.InfluxDBClient") as mock_client_cls, patch(
        "sinks.influx_sink.Point"
    ) as mock_point_cls:
        mock_client = MagicMock()
        mock_write_api = MagicMock()
        mock_client.write_api.return_value = mock_write_api
        mock_client_cls.return_value = mock_client

        point = MagicMock()
        point.tag.return_value = point
        point.field.return_value = point
        point.time.return_value = point
        mock_point_cls.return_value = point

        sink = InfluxSink(settings)
        event = {
            "payload": {
                "bin_id": "BIN-001",
                "fill_level_pct": 77.7,
                "battery_level_pct": 66,
                "timestamp": "2026-04-24T12:00:00Z",
            }
        }

        sink.write_raw_event(event)

        mock_write_api.write.assert_called_once()
