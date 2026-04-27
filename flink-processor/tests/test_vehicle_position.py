from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from kafka.errors import NoBrokersAvailable

from config import load_settings
import job_vehicle
from processors.vehicle_position import ValidationError, VehiclePositionProcessor
from sinks.influx_sink import InfluxSink


@pytest.fixture
def processor():
    return VehiclePositionProcessor()


@pytest.fixture
def valid_message():
    return {
        "vehicle_id": "LORRY-01",
        "latitude": 6.9271,
        "longitude": 79.8612,
        "timestamp": "2026-04-22T10:00:00Z",
        "job_id": "JOB-001",
    }


def test_valid_gps_message(processor, valid_message):
    parsed = processor.parse_kafka_message(valid_message)

    assert parsed["vehicle_id"] == "LORRY-01"
    assert parsed["latitude"] == 6.9271
    assert parsed["longitude"] == 79.8612
    assert parsed["job_id"] == "JOB-001"
    assert parsed["timestamp"] == datetime(2026, 4, 22, 10, 0, tzinfo=timezone.utc)


def test_missing_vehicle_id(processor, valid_message):
    del valid_message["vehicle_id"]

    with pytest.raises(ValidationError, match="Missing required fields"):
        processor.parse_kafka_message(valid_message)


def test_invalid_latitude(processor, valid_message):
    valid_message["latitude"] = 91

    with pytest.raises(ValidationError, match="latitude must be between -90 and 90"):
        processor.parse_kafka_message(valid_message)


def test_invalid_longitude(processor, valid_message):
    valid_message["longitude"] = -181

    with pytest.raises(ValidationError, match="longitude must be between -180 and 180"):
        processor.parse_kafka_message(valid_message)


def test_missing_timestamp(processor, valid_message):
    del valid_message["timestamp"]

    with pytest.raises(ValidationError, match="Missing required fields"):
        processor.parse_kafka_message(valid_message)


def test_malformed_json(processor):
    with pytest.raises(ValidationError, match="Malformed JSON"):
        processor.parse_kafka_message('{"vehicle_id": "LORRY-01",')


def test_influx_disabled_logs_instead_of_write(monkeypatch, caplog, valid_message):
    monkeypatch.setenv("INFLUX_ENABLED", "false")
    settings = load_settings()

    with patch("sinks.influx_sink.InfluxDBClient") as mock_client_cls:
        sink = InfluxSink(settings)
        assert mock_client_cls.call_count == 0

        caplog.set_level("INFO")
        sink.write_vehicle_position(valid_message)

        assert "Influx disabled; vehicle position event:" in caplog.text
        assert "LORRY-01" in caplog.text


def test_vehicle_position_sink_writes_point_when_enabled():
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
        sink.write_vehicle_position(
            {
                "vehicle_id": "LORRY-01",
                "latitude": 6.9271,
                "longitude": 79.8612,
                "timestamp": "2026-04-22T10:00:00Z",
                "job_id": "JOB-001",
            }
        )

        mock_write_api.write.assert_called_once()


def test_run_kafka_mode_handles_no_brokers(monkeypatch, caplog):
    settings = load_settings()
    processor = VehiclePositionProcessor()

    monkeypatch.setattr(job_vehicle, "_build_kafka_consumer", lambda _settings: (_ for _ in ()).throw(NoBrokersAvailable()))

    with patch("sinks.influx_sink.InfluxDBClient"):
        influx_sink = InfluxSink(settings)

    caplog.set_level("ERROR")
    job_vehicle.run_kafka_mode(
        settings=settings,
        processor=processor,
        influx_sink=influx_sink,
        logger=job_vehicle.logging.getLogger("test-pipeline-4"),
        max_messages=10,
    )

    assert "Kafka broker unavailable for topic" in caplog.text