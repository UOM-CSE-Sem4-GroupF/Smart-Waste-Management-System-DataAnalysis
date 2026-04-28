import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional


logger = logging.getLogger(__name__)


class ValidationError(Exception):
    """Raised when processed-bin event validation fails."""


class ZoneAggregationProcessor:
    REQUIRED_FIELDS = {
        "zone_id",
        "event_ts",
        "fill_level_pct",
        "urgency_score",
        "estimated_weight_kg",
    }

    def __init__(self, window_minutes: int = 10, slide_minutes: int = 2) -> None:
        self.window_minutes = window_minutes
        self.slide_minutes = slide_minutes
        self.window_delta = timedelta(minutes=window_minutes)
        self.slide_delta = timedelta(minutes=slide_minutes)
        self._events_by_zone: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
        self._last_emitted_window_end: Dict[int, datetime] = {}

    @staticmethod
    def _parse_timestamp(value: Any) -> datetime:
        if isinstance(value, datetime):
            ts = value
        elif isinstance(value, str):
            try:
                ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError as exc:
                raise ValidationError(f"Invalid event_ts format: {value}") from exc
        else:
            raise ValidationError("event_ts must be datetime or ISO timestamp string")

        if ts.tzinfo is None:
            return ts.replace(tzinfo=timezone.utc)
        return ts.astimezone(timezone.utc)

    @staticmethod
    def _floor_to_slide(ts: datetime, slide_minutes: int) -> datetime:
        step_seconds = slide_minutes * 60
        epoch_seconds = int(ts.timestamp())
        floored = epoch_seconds - (epoch_seconds % step_seconds)
        return datetime.fromtimestamp(floored, tz=timezone.utc)

    def parse_processed_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(event, dict):
            raise ValidationError(f"Expected dict event, got {type(event)}")

        missing = self.REQUIRED_FIELDS - set(event.keys())
        if missing:
            raise ValidationError(f"Missing required fields: {missing}")

        zone_id = event.get("zone_id")
        if not isinstance(zone_id, int):
            raise ValidationError("zone_id must be integer")

        fill_level = event.get("fill_level_pct")
        if not isinstance(fill_level, (int, float)):
            raise ValidationError("fill_level_pct must be numeric")

        urgency_score = event.get("urgency_score")
        if not isinstance(urgency_score, (int, float)):
            raise ValidationError("urgency_score must be numeric")

        estimated_weight = event.get("estimated_weight_kg")
        if not isinstance(estimated_weight, (int, float)):
            raise ValidationError("estimated_weight_kg must be numeric")

        waste_category_id = event.get("waste_category_id")
        if waste_category_id is not None and not isinstance(waste_category_id, int):
            raise ValidationError("waste_category_id must be integer when provided")

        parsed = dict(event)
        parsed["zone_id"] = int(zone_id)
        parsed["event_ts"] = self._parse_timestamp(event.get("event_ts"))
        parsed["fill_level_pct"] = float(fill_level)
        parsed["urgency_score"] = float(urgency_score)
        parsed["estimated_weight_kg"] = float(estimated_weight)
        parsed["waste_category_id"] = waste_category_id
        return parsed

    def build_zone_snapshot(
        self,
        zone_id: int,
        window_start: datetime,
        window_end: datetime,
        records: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        total_bins = len(records)
        if total_bins == 0:
            raise ValidationError("Cannot build zone snapshot from empty records")

        avg_fill_level = sum(r["fill_level_pct"] for r in records) / total_bins
        urgent_bin_count = sum(1 for r in records if r["urgency_score"] > 60.0)
        critical_bin_count = sum(1 for r in records if r["urgency_score"] > 85.0)
        total_estimated_kg = sum(r["estimated_weight_kg"] for r in records)

        breakdown: Dict[str, int] = {}
        for record in records:
            category_id = record.get("waste_category_id")
            if category_id is None:
                continue
            key = str(category_id)
            breakdown[key] = breakdown.get(key, 0) + 1

        dominant_waste_category: Optional[str] = None
        if breakdown:
            dominant_waste_category = min(
                (k for k, v in breakdown.items() if v == max(breakdown.values())),
                key=lambda k: int(k),
            )

        return {
            "zone_id": zone_id,
            "window_start": window_start,
            "snapshot_at": window_end,
            "window_minutes": self.window_minutes,
            "avg_fill_level": round(avg_fill_level, 2),
            "urgent_bin_count": urgent_bin_count,
            "critical_bin_count": critical_bin_count,
            "total_bins": total_bins,
            "total_estimated_kg": round(total_estimated_kg, 2),
            "waste_category_breakdown": breakdown,
            "dominant_waste_category": dominant_waste_category,
        }

    def process(self, event: Dict[str, Any]) -> List[Dict[str, Any]]:
        parsed = self.parse_processed_event(event)
        zone_id = parsed["zone_id"]
        event_ts = parsed["event_ts"]

        events = self._events_by_zone[zone_id]
        events.append(parsed)

        cutoff = event_ts - self.window_delta
        self._events_by_zone[zone_id] = [r for r in events if r["event_ts"] > cutoff]

        latest_window_end = self._floor_to_slide(event_ts, self.slide_minutes)
        previous = self._last_emitted_window_end.get(zone_id)
        if previous is None:
            previous = latest_window_end - self.slide_delta

        snapshots: List[Dict[str, Any]] = []
        cursor = previous + self.slide_delta
        zone_events = self._events_by_zone[zone_id]

        while cursor <= latest_window_end:
            window_start = cursor - self.window_delta
            window_records = [
                r for r in zone_events if window_start < r["event_ts"] <= cursor
            ]
            if window_records:
                snapshots.append(
                    self.build_zone_snapshot(
                        zone_id=zone_id,
                        window_start=window_start,
                        window_end=cursor,
                        records=window_records,
                    )
                )
            cursor += self.slide_delta

        self._last_emitted_window_end[zone_id] = latest_window_end
        return snapshots