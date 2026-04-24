from datetime import datetime
from typing import Any, Dict, Optional

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

from config import Settings


class InfluxSink:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._client = InfluxDBClient(
            url=self.settings.influx_url,
            token=self.settings.influx_token,
            org=self.settings.influx_org,
        )
        self._write_api = self._client.write_api(write_options=SYNCHRONOUS)

    @staticmethod
    def _parse_timestamp(value: Any) -> Optional[datetime]:
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                return None
        return None

    def write_raw_event(self, event: Dict[str, Any]) -> None:
        payload = event.get("payload", {})
        point = Point("bin_readings_raw")
        if payload.get("bin_id") is not None:
            point.tag("bin_id", str(payload.get("bin_id")))
        if payload.get("zone_id") is not None:
            point.tag("zone_id", str(payload.get("zone_id")))
        if payload.get("waste_category") is not None:
            point.tag("waste_category", str(payload.get("waste_category")))

        if isinstance(payload.get("fill_level_pct"), (int, float)):
            point.field("fill_level_pct", float(payload["fill_level_pct"]))
        if isinstance(payload.get("battery_level_pct"), (int, float)):
            point.field("battery_level_pct", float(payload["battery_level_pct"]))
        if isinstance(payload.get("signal_strength"), (int, float)):
            point.field("signal_strength", float(payload["signal_strength"]))
        if isinstance(payload.get("temperature_c"), (int, float)):
            point.field("temperature_c", float(payload["temperature_c"]))

        ts = self._parse_timestamp(payload.get("timestamp"))
        if ts is not None:
            point.time(ts, WritePrecision.MS)

        self._write_api.write(
            bucket=self.settings.influx_raw_bucket,
            org=self.settings.influx_org,
            record=point,
        )

    def write_processed_event(self, event: Dict[str, Any]) -> None:
        point = Point("bin_readings_processed")

        if event.get("bin_id") is not None:
            point.tag("bin_id", str(event.get("bin_id")))
        if event.get("zone_id") is not None:
            point.tag("zone_id", str(event.get("zone_id")))
        waste_category_tag = event.get("waste_category")
        if waste_category_tag is None and event.get("waste_category_id") is not None:
            waste_category_tag = str(event.get("waste_category_id"))
        if waste_category_tag is not None:
            point.tag("waste_category", str(waste_category_tag))
        if event.get("status") is not None:
            point.tag("status", str(event.get("status")))

        if isinstance(event.get("fill_level_pct"), (int, float)):
            point.field("fill_level_pct", float(event["fill_level_pct"]))
        if isinstance(event.get("urgency_score"), int):
            point.field("urgency_score", int(event["urgency_score"]))
        if isinstance(event.get("estimated_weight_kg"), (int, float)):
            point.field("estimated_weight_kg", float(event["estimated_weight_kg"]))
        if isinstance(event.get("fill_rate_pct_per_hour"), (int, float)):
            point.field("fill_rate_pct_per_hour", float(event["fill_rate_pct_per_hour"]))

        predicted_full_at = self._parse_timestamp(event.get("predicted_full_at"))
        event_ts = self._parse_timestamp(event.get("event_ts"))
        if predicted_full_at is not None and event_ts is not None:
            delta_hours = (predicted_full_at - event_ts).total_seconds() / 3600.0
            point.field("predicted_full_hours", float(delta_hours))

        if event_ts is not None:
            point.time(event_ts, WritePrecision.MS)

        self._write_api.write(
            bucket=self.settings.influx_processed_bucket,
            org=self.settings.influx_org,
            record=point,
        )

    def write_zone_statistics(self, event: Dict[str, Any]) -> None:
        point = Point("zone_statistics")

        if event.get("zone_id") is not None:
            point.tag("zone_id", str(event.get("zone_id")))
        if event.get("dominant_waste_category") is not None:
            point.tag("dominant_waste_category", str(event.get("dominant_waste_category")))

        if isinstance(event.get("avg_fill_level"), (int, float)):
            point.field("avg_fill_level", float(event["avg_fill_level"]))
        if isinstance(event.get("urgent_bin_count"), int):
            point.field("urgent_bin_count", int(event["urgent_bin_count"]))
        if isinstance(event.get("critical_bin_count"), int):
            point.field("critical_bin_count", int(event["critical_bin_count"]))
        if isinstance(event.get("total_bins"), int):
            point.field("total_bins", int(event["total_bins"]))
        if isinstance(event.get("total_estimated_kg"), (int, float)):
            point.field("total_estimated_kg", float(event["total_estimated_kg"]))
        if isinstance(event.get("window_minutes"), int):
            point.field("window_minutes", int(event["window_minutes"]))

        breakdown = event.get("waste_category_breakdown")
        if isinstance(breakdown, dict):
            for category_id, count in breakdown.items():
                if isinstance(count, int):
                    point.field(f"category_{category_id}_count", count)

        snapshot_ts = self._parse_timestamp(event.get("snapshot_at"))
        if snapshot_ts is not None:
            point.time(snapshot_ts, WritePrecision.MS)

        self._write_api.write(
            bucket=self.settings.influx_zone_bucket,
            org=self.settings.influx_org,
            record=point,
        )

    def close(self) -> None:
        self._client.close()
