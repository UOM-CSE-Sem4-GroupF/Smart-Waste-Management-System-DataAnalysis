import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch

from config import load_settings
from metadata_store import MetadataStore
from processors.bin_telemetry import BinTelemetryProcessor, ValidationError


@pytest.fixture
def settings():
    return load_settings()


@pytest.fixture
def processor(settings):
    with patch("metadata_store.pool.SimpleConnectionPool"):
        metadata_store = MetadataStore(settings)
    return BinTelemetryProcessor(metadata_store)


# ============================================================================
# KAFKA MESSAGE PARSING AND VALIDATION TESTS
# ============================================================================


class TestKafkaMessageParsing:
    """Tests for JSON parsing and validation of Kafka messages."""

    def test_parse_valid_event(self, processor):
        """Valid event should parse successfully."""
        raw = {
            "payload": {
                "bin_id": "BIN-001",
                "fill_level_pct": 75.5,
                "battery_level_pct": 85.0,
                "timestamp": "2026-04-24T12:00:00Z"
            }
        }
        
        event = processor.parse_kafka_event(raw)
        
        assert event.event_id.startswith("BIN-001_")
        assert event.payload["bin_id"] == "BIN-001"
        assert event.payload["fill_level_pct"] == 75.5
        assert event.payload["battery_level_pct"] == 85.0
        assert isinstance(event.event_ts, datetime)

    def test_parse_valid_event_with_extra_fields(self, processor):
        """Valid event with extra optional fields should parse."""
        raw = {
            "payload": {
                "bin_id": "BIN-002",
                "fill_level_pct": 50,
                "battery_level_pct": 90,
                "timestamp": "2026-04-24T15:30:00Z",
                "temperature_c": 28.3,
                "signal_strength": -75,
            }
        }
        
        event = processor.parse_kafka_event(raw)
        
        assert event.payload["bin_id"] == "BIN-002"
        assert event.payload.get("temperature_c") == 28.3

    def test_parse_missing_payload_key(self, processor):
        """Missing 'payload' key should raise ValidationError."""
        raw = {"bin_id": "BIN-001"}
        
        with pytest.raises(ValidationError, match="Missing 'payload' key"):
            processor.parse_kafka_event(raw)

    def test_parse_null_payload(self, processor):
        """Null payload should raise ValidationError."""
        raw = {"payload": None}
        
        with pytest.raises(ValidationError, match="Missing 'payload' key"):
            processor.parse_kafka_event(raw)

    def test_parse_payload_not_dict(self, processor):
        """Payload that is not a dict should raise ValidationError."""
        raw = {"payload": "not a dict"}
        
        with pytest.raises(ValidationError, match="'payload' must be dict"):
            processor.parse_kafka_event(raw)

    def test_parse_missing_bin_id(self, processor):
        """Missing bin_id should raise ValidationError."""
        raw = {
            "payload": {
                "fill_level_pct": 75,
                "battery_level_pct": 85,
                "timestamp": "2026-04-24T12:00:00Z"
            }
        }
        
        with pytest.raises(ValidationError, match="Missing required fields"):
            processor.parse_kafka_event(raw)

    def test_parse_bin_id_empty_string(self, processor):
        """Empty bin_id should raise ValidationError."""
        raw = {
            "payload": {
                "bin_id": "",
                "fill_level_pct": 75,
                "battery_level_pct": 85,
                "timestamp": "2026-04-24T12:00:00Z"
            }
        }
        
        with pytest.raises(ValidationError, match="bin_id must be non-empty string"):
            processor.parse_kafka_event(raw)

    def test_parse_bin_id_not_string(self, processor):
        """Non-string bin_id should raise ValidationError."""
        raw = {
            "payload": {
                "bin_id": 123,
                "fill_level_pct": 75,
                "battery_level_pct": 85,
                "timestamp": "2026-04-24T12:00:00Z"
            }
        }
        
        with pytest.raises(ValidationError, match="bin_id must be non-empty string"):
            processor.parse_kafka_event(raw)

    def test_parse_fill_level_not_numeric(self, processor):
        """Non-numeric fill_level_pct should raise ValidationError."""
        raw = {
            "payload": {
                "bin_id": "BIN-001",
                "fill_level_pct": "seventy-five",
                "battery_level_pct": 85,
                "timestamp": "2026-04-24T12:00:00Z"
            }
        }
        
        with pytest.raises(ValidationError, match="fill_level_pct must be numeric"):
            processor.parse_kafka_event(raw)

    def test_parse_fill_level_negative(self, processor):
        """Negative fill_level_pct should raise ValidationError."""
        raw = {
            "payload": {
                "bin_id": "BIN-001",
                "fill_level_pct": -10,
                "battery_level_pct": 85,
                "timestamp": "2026-04-24T12:00:00Z"
            }
        }
        
        with pytest.raises(ValidationError, match="fill_level_pct must be 0-100"):
            processor.parse_kafka_event(raw)

    def test_parse_fill_level_over_100(self, processor):
        """fill_level_pct > 100 should raise ValidationError."""
        raw = {
            "payload": {
                "bin_id": "BIN-001",
                "fill_level_pct": 105,
                "battery_level_pct": 85,
                "timestamp": "2026-04-24T12:00:00Z"
            }
        }
        
        with pytest.raises(ValidationError, match="fill_level_pct must be 0-100"):
            processor.parse_kafka_event(raw)

    def test_parse_fill_level_zero(self, processor):
        """fill_level_pct = 0 should be valid."""
        raw = {
            "payload": {
                "bin_id": "BIN-001",
                "fill_level_pct": 0,
                "battery_level_pct": 85,
                "timestamp": "2026-04-24T12:00:00Z"
            }
        }
        
        event = processor.parse_kafka_event(raw)
        assert event.payload["fill_level_pct"] == 0

    def test_parse_fill_level_100(self, processor):
        """fill_level_pct = 100 should be valid."""
        raw = {
            "payload": {
                "bin_id": "BIN-001",
                "fill_level_pct": 100,
                "battery_level_pct": 85,
                "timestamp": "2026-04-24T12:00:00Z"
            }
        }
        
        event = processor.parse_kafka_event(raw)
        assert event.payload["fill_level_pct"] == 100

    def test_parse_timestamp_invalid_format(self, processor):
        """Invalid timestamp format should raise ValidationError."""
        raw = {
            "payload": {
                "bin_id": "BIN-001",
                "fill_level_pct": 75,
                "battery_level_pct": 85,
                "timestamp": "not-a-timestamp"
            }
        }
        
        with pytest.raises(ValidationError, match="Invalid ISO timestamp format"):
            processor.parse_kafka_event(raw)


# ============================================================================
# METADATA STORE TESTS
# ============================================================================


class TestMetadataStore:
    """Tests for PostgreSQL metadata enrichment using real schema."""

    @patch("metadata_store.pool.SimpleConnectionPool")
    def test_get_bin_metadata_success(self, mock_pool_class, settings):
        """Valid bin_id should return enriched metadata from real schema."""
        mock_pool = MagicMock()
        mock_pool_class.return_value = mock_pool
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_pool.getconn.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock row from real schema JOIN query
        mock_cursor.fetchone.return_value = (
            "BIN-001", 1, 6.927079, 79.861244, 240.0, 1, 0.9
        )
        
        store = MetadataStore(settings)
        metadata = store.get_bin_metadata("BIN-001")
        
        assert metadata is not None
        assert metadata["bin_id"] == "BIN-001"
        assert metadata["zone_id"] == 1
        assert metadata["latitude"] == 6.927079
        assert metadata["longitude"] == 79.861244
        assert metadata["volume_litres"] == 240.0
        assert metadata["waste_category_id"] == 1
        assert metadata["avg_kg_per_litre"] == 0.9

    @patch("metadata_store.pool.SimpleConnectionPool")
    def test_get_bin_metadata_not_found(self, mock_pool_class, settings):
        """Non-existent bin_id should return None."""
        mock_pool = MagicMock()
        mock_pool_class.return_value = mock_pool
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_pool.getconn.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = None
        
        store = MetadataStore(settings)
        metadata = store.get_bin_metadata("NONEXISTENT")
        
        assert metadata is None

    @patch("metadata_store.pool.SimpleConnectionPool")
    def test_get_bin_metadata_cache_hit(self, mock_pool_class, settings):
        """Second call should use cache."""
        mock_pool = MagicMock()
        mock_pool_class.return_value = mock_pool
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_pool.getconn.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = (
            "BIN-001", 1, 6.927079, 79.861244, 240.0, 1, 0.9
        )
        
        store = MetadataStore(settings)
        metadata1 = store.get_bin_metadata("BIN-001")
        mock_cursor.reset_mock()
        metadata2 = store.get_bin_metadata("BIN-001")
        
        assert metadata2 == metadata1
        # Second call should not hit cursor (cache hit)
        mock_cursor.assert_not_called()

    @patch("metadata_store.pool.SimpleConnectionPool")
    def test_get_bin_metadata_invalid_bin_id_none(self, mock_pool_class, settings):
        """None bin_id should return None."""
        mock_pool = MagicMock()
        mock_pool_class.return_value = mock_pool
        
        store = MetadataStore(settings)
        metadata = store.get_bin_metadata(None)
        
        assert metadata is None
        # DB should not be queried
        mock_pool.getconn.assert_not_called()

    @patch("metadata_store.pool.SimpleConnectionPool")
    def test_get_bin_metadata_with_null_fields(self, mock_pool_class, settings):
        """Null fields should convert to None."""
        mock_pool = MagicMock()
        mock_pool_class.return_value = mock_pool
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_pool.getconn.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        mock_cursor.fetchone.return_value = (
            "BIN-001", 1, 6.927079, 79.861244, 240.0, None, None
        )
        
        store = MetadataStore(settings)
        metadata = store.get_bin_metadata("BIN-001")
        
        assert metadata is not None
        assert metadata["waste_category_id"] is None
        assert metadata["avg_kg_per_litre"] is None


# ============================================================================
# PROCESSING LOGIC TESTS (PHASE 4)
# ============================================================================


class TestProcessingLogic:
    """Tests for enrichment + weight and urgency calculations."""

    def test_process_computes_weight_and_status(self):
        metadata_store = MagicMock()
        metadata_store.get_bin_metadata.return_value = {
            "bin_id": "BIN-001",
            "zone_id": 1,
            "latitude": 6.927079,
            "longitude": 79.861244,
            "volume_litres": 240.0,
            "waste_category_id": 1,
            "avg_kg_per_litre": 0.9,
        }
        processor = BinTelemetryProcessor(metadata_store)

        raw = {
            "payload": {
                "bin_id": "BIN-001",
                "fill_level_pct": 60,
                "battery_level_pct": 88,
                "timestamp": "2026-04-24T12:00:00Z",
            }
        }

        processed = processor.process(raw)

        assert processed is not None
        assert processed["status"] == "monitor"
        assert processed["urgency_score"] == 60
        assert processed["estimated_weight_kg"] == pytest.approx(12960.0)
        assert processed["zone_id"] == 1
        assert processed["anomaly_detected"] is False
        assert processed["anomaly_flags"] == []

    @pytest.mark.parametrize(
        "fill_level,expected_status",
        [
            (49.99, "normal"),
            (50, "monitor"),
            (74.99, "monitor"),
            (75, "urgent"),
            (90, "urgent"),
            (90.01, "critical"),
        ],
    )
    def test_process_status_boundaries(self, fill_level, expected_status):
        metadata_store = MagicMock()
        metadata_store.get_bin_metadata.return_value = {
            "bin_id": "BIN-001",
            "zone_id": 1,
            "latitude": 6.0,
            "longitude": 79.0,
            "volume_litres": 1.0,
            "waste_category_id": 1,
            "avg_kg_per_litre": 1.0,
        }
        processor = BinTelemetryProcessor(metadata_store)

        raw = {
            "payload": {
                "bin_id": "BIN-001",
                "fill_level_pct": fill_level,
                "battery_level_pct": 70,
                "timestamp": "2026-04-24T12:00:00Z",
            }
        }

        processed = processor.process(raw)

        assert processed is not None
        assert processed["status"] == expected_status

    def test_process_returns_none_when_metadata_missing(self):
        metadata_store = MagicMock()
        metadata_store.get_bin_metadata.return_value = None
        processor = BinTelemetryProcessor(metadata_store)

        raw = {
            "payload": {
                "bin_id": "BIN-404",
                "fill_level_pct": 70,
                "battery_level_pct": 80,
                "timestamp": "2026-04-24T12:00:00Z",
            }
        }

        processed = processor.process(raw)

        assert processed is None
        metadata_store.get_bin_metadata.assert_called_once_with("BIN-404")


# ============================================================================
# ANOMALY LOGIC TESTS (PHASE 5)
# ============================================================================


class TestAnomalyLogic:
    """Tests for anomaly flags emitted during processing."""

    def _make_processor(self):
        metadata_store = MagicMock()
        metadata_store.get_bin_metadata.return_value = {
            "bin_id": "BIN-001",
            "zone_id": 1,
            "latitude": 6.9,
            "longitude": 79.8,
            "volume_litres": 120.0,
            "waste_category_id": 1,
            "avg_kg_per_litre": 0.8,
        }
        return BinTelemetryProcessor(metadata_store)

    def test_anomaly_low_battery(self):
        processor = self._make_processor()
        raw = {
            "payload": {
                "bin_id": "BIN-001",
                "fill_level_pct": 65,
                "battery_level_pct": 10,
                "timestamp": "2026-04-24T12:00:00Z",
            }
        }

        processed = processor.process(raw)

        assert processed is not None
        assert processed["anomaly_detected"] is True
        assert "low_battery" in processed["anomaly_flags"]

    def test_anomaly_weak_signal(self):
        processor = self._make_processor()
        raw = {
            "payload": {
                "bin_id": "BIN-001",
                "fill_level_pct": 40,
                "battery_level_pct": 60,
                "signal_strength": -110,
                "timestamp": "2026-04-24T12:00:00Z",
            }
        }

        processed = processor.process(raw)

        assert processed is not None
        assert processed["anomaly_detected"] is True
        assert "weak_signal" in processed["anomaly_flags"]

    def test_anomaly_abnormal_temperature(self):
        processor = self._make_processor()
        raw = {
            "payload": {
                "bin_id": "BIN-001",
                "fill_level_pct": 40,
                "battery_level_pct": 60,
                "temperature_c": 85,
                "timestamp": "2026-04-24T12:00:00Z",
            }
        }

        processed = processor.process(raw)

        assert processed is not None
        assert processed["anomaly_detected"] is True
        assert "abnormal_temperature" in processed["anomaly_flags"]
