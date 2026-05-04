from datetime import datetime, timezone

import pytest

from processors.zone_aggregation import ValidationError, ZoneAggregationProcessor


def _event(
    *,
    zone_id=1,
    event_ts="2026-04-24T12:04:00Z",
    fill_level_pct=75.0,
    urgency_score=75,
    estimated_weight_kg=12.5,
    waste_category_id=1,
):
    return {
        "zone_id": zone_id,
        "event_ts": event_ts,
        "fill_level_pct": fill_level_pct,
        "urgency_score": urgency_score,
        "estimated_weight_kg": estimated_weight_kg,
        "waste_category_id": waste_category_id,
    }


def test_valid_processed_bin_event():
    processor = ZoneAggregationProcessor()
    parsed = processor.parse_processed_event(_event())

    assert parsed["zone_id"] == 1
    assert parsed["fill_level_pct"] == 75.0
    assert parsed["urgency_score"] == 75.0
    assert parsed["estimated_weight_kg"] == 12.5
    assert parsed["event_ts"] == datetime(2026, 4, 24, 12, 4, tzinfo=timezone.utc)


def test_missing_zone_id():
    processor = ZoneAggregationProcessor()
    event = _event()
    del event["zone_id"]

    with pytest.raises(ValidationError, match="Missing required fields"):
        processor.parse_processed_event(event)


def test_missing_fill_level():
    processor = ZoneAggregationProcessor()
    event = _event()
    del event["fill_level_pct"]

    with pytest.raises(ValidationError, match="Missing required fields"):
        processor.parse_processed_event(event)


def test_urgency_count_logic():
    processor = ZoneAggregationProcessor()
    records = [
        processor.parse_processed_event(_event(urgency_score=60)),
        processor.parse_processed_event(_event(urgency_score=61, event_ts="2026-04-24T12:03:00Z")),
        processor.parse_processed_event(_event(urgency_score=95, event_ts="2026-04-24T12:02:00Z")),
    ]

    snapshot = processor.build_zone_snapshot(
        zone_id=1,
        window_start=datetime(2026, 4, 24, 11, 54, tzinfo=timezone.utc),
        window_end=datetime(2026, 4, 24, 12, 4, tzinfo=timezone.utc),
        records=records,
    )

    assert snapshot["urgent_bin_count"] == 2


def test_critical_count_logic():
    processor = ZoneAggregationProcessor()
    records = [
        processor.parse_processed_event(_event(urgency_score=85)),
        processor.parse_processed_event(_event(urgency_score=86, event_ts="2026-04-24T12:03:00Z")),
        processor.parse_processed_event(_event(urgency_score=99, event_ts="2026-04-24T12:02:00Z")),
    ]

    snapshot = processor.build_zone_snapshot(
        zone_id=1,
        window_start=datetime(2026, 4, 24, 11, 54, tzinfo=timezone.utc),
        window_end=datetime(2026, 4, 24, 12, 4, tzinfo=timezone.utc),
        records=records,
    )

    assert snapshot["critical_bin_count"] == 2


def test_total_weight_calculation():
    processor = ZoneAggregationProcessor()
    records = [
        processor.parse_processed_event(_event(estimated_weight_kg=10.25)),
        processor.parse_processed_event(_event(estimated_weight_kg=5.75, event_ts="2026-04-24T12:03:00Z")),
        processor.parse_processed_event(_event(estimated_weight_kg=1.0, event_ts="2026-04-24T12:02:00Z")),
    ]

    snapshot = processor.build_zone_snapshot(
        zone_id=1,
        window_start=datetime(2026, 4, 24, 11, 54, tzinfo=timezone.utc),
        window_end=datetime(2026, 4, 24, 12, 4, tzinfo=timezone.utc),
        records=records,
    )

    assert snapshot["total_estimated_kg"] == 17.0


def test_waste_category_breakdown():
    processor = ZoneAggregationProcessor()
    records = [
        processor.parse_processed_event(_event(waste_category_id=2)),
        processor.parse_processed_event(_event(waste_category_id=2, event_ts="2026-04-24T12:03:00Z")),
        processor.parse_processed_event(_event(waste_category_id=1, event_ts="2026-04-24T12:02:00Z")),
    ]

    snapshot = processor.build_zone_snapshot(
        zone_id=1,
        window_start=datetime(2026, 4, 24, 11, 54, tzinfo=timezone.utc),
        window_end=datetime(2026, 4, 24, 12, 4, tzinfo=timezone.utc),
        records=records,
    )

    assert snapshot["waste_category_breakdown"] == {"2": 2, "1": 1}
    assert snapshot["dominant_waste_category"] == "2"