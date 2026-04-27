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
    """Tests for enrichment + weighted priority score calculations."""

    @staticmethod
    def _base_metadata():
        return {
            "bin_id": "BIN-001",
            "zone_id": 1,
            "latitude": 6.927079,
            "longitude": 79.861244,
            "volume_litres": 240.0,
            "waste_category_id": 1,
            "avg_kg_per_litre": 0.9,
        }

    @staticmethod
    def _base_payload():
        return {
            "bin_id": "BIN-001",
            "fill_level_pct": 40,
            "battery_level_pct": 88,
            "timestamp": "2026-04-24T12:00:00Z",
        }

    def _process(self, payload_overrides=None, metadata_overrides=None):
        metadata_store = MagicMock()
        metadata = self._base_metadata()
        if metadata_overrides:
            metadata.update(metadata_overrides)
        metadata_store.get_bin_metadata.return_value = metadata

        processor = BinTelemetryProcessor(metadata_store)

        payload = self._base_payload()
        if payload_overrides:
            payload.update(payload_overrides)

        return processor.process({"payload": payload})

    def test_process_computes_weight_and_status(self):
        processed = self._process(
            payload_overrides={
                "fill_level_pct": 80,
                "fill_rate_pct_per_hour": 4.2,
                "predicted_full_at": "2026-04-24T14:30:00Z",
                "alerts": [],
            }
        )

        assert processed is not None
        assert processed["status"] == "monitor"
        assert processed["urgency_score"] == processed["priority_score"]
        assert 0 <= processed["priority_score"] <= 100
        assert processed["estimated_weight_kg"] == pytest.approx(17280.0)
        assert processed["zone_id"] == 1
        assert processed["fill_rate_pct_per_hour"] == 4.2
        assert processed["predicted_full_at"] == "2026-04-24T14:30:00Z"
        assert processed["timestamp"] == "2026-04-24T12:00:00Z"
        assert processed["anomaly_detected"] is False
        assert processed["anomaly_flags"] == []

    def test_high_fill_level_increases_priority(self):
        low_fill = self._process(payload_overrides={"fill_level_pct": 20})
        high_fill = self._process(payload_overrides={"fill_level_pct": 90})

        assert low_fill is not None
        assert high_fill is not None
        assert high_fill["priority_score"] > low_fill["priority_score"]

    def test_long_time_since_collection_increases_priority(self):
        short_time = self._process(payload_overrides={"hours_since_last_collection": 2})
        long_time = self._process(payload_overrides={"hours_since_last_collection": 24})

        assert short_time is not None
        assert long_time is not None
        assert long_time["priority_score"] > short_time["priority_score"]

    def test_soon_predicted_full_increases_priority(self):
        later_full = self._process(payload_overrides={"hours_until_full": 30})
        soon_full = self._process(payload_overrides={"hours_until_full": 1})

        assert later_full is not None
        assert soon_full is not None
        assert soon_full["priority_score"] > later_full["priority_score"]

    def test_high_distance_cost_increases_priority(self):
        near_vehicle = self._process(payload_overrides={"distance_to_nearest_vehicle_km": 1})
        far_vehicle = self._process(payload_overrides={"distance_to_nearest_vehicle_km": 10})

        assert near_vehicle is not None
        assert far_vehicle is not None
        assert far_vehicle["priority_score"] > near_vehicle["priority_score"]

    def test_high_risk_factor_increases_priority(self):
        low_risk = self._process(payload_overrides={"risk_factor": 10})
        high_risk = self._process(payload_overrides={"risk_factor": 95})

        assert low_risk is not None
        assert high_risk is not None
        assert high_risk["priority_score"] > low_risk["priority_score"]

    @pytest.mark.parametrize(
        "payload_overrides,expected_status",
        [
            ({"fill_level_pct": 20}, "normal"),
            ({"fill_level_pct": 80}, "monitor"),
            ({"fill_level_pct": 80, "hours_since_last_collection": 24, "hours_until_full": 6}, "urgent"),
            (
                {
                    "fill_level_pct": 100,
                    "hours_since_last_collection": 24,
                    "hours_until_full": 1,
                    "distance_to_nearest_vehicle_km": 10,
                    "risk_factor": 100,
                },
                "critical",
            ),
        ],
    )
    def test_process_status_boundaries(self, payload_overrides, expected_status):
        processed = self._process(payload_overrides=payload_overrides)

        assert processed is not None
        assert processed["status"] == expected_status

    def test_priority_score_is_clamped_between_0_and_100(self):
        low = self._process(
            payload_overrides={
                "fill_level_pct": 0,
                "hours_since_last_collection": -100,
                "hours_until_full": 100,
                "distance_to_nearest_vehicle_km": -50,
                "risk_factor": -10,
            }
        )
        high = self._process(
            payload_overrides={
                "fill_level_pct": 100,
                "hours_since_last_collection": 999,
                "hours_until_full": -1,
                "distance_to_nearest_vehicle_km": 999,
                "risk_factor": 999,
            }
        )

        assert low is not None
        assert high is not None
        assert 0 <= low["priority_score"] <= 100
        assert 0 <= high["priority_score"] <= 100

    def test_urgency_score_equals_priority_score(self):
        processed = self._process(
            payload_overrides={
                "fill_level_pct": 82,
                "hours_since_last_collection": 12,
                "hours_until_full": 3,
                "distance_to_nearest_vehicle_km": 4,
                "risk_factor": 35,
            }
        )

        assert processed is not None
        assert processed["urgency_score"] == processed["priority_score"]

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
