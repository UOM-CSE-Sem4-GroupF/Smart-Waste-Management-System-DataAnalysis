import json
import os
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from ...config import load_settings
except ImportError:
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))
    from config import load_settings

from .check_test_outputs import (
    check_influx_for_measurement,
    check_kafka_alert_for_deviation,
    check_kafka_bin_processed,
    check_kafka_topic_for_test_run,
    check_kafka_zone_snapshot,
    check_postgres_bin_state_updated,
    check_postgres_zone_snapshot_since,
    create_kafka_watcher,
    get_bins_for_zone,
    get_route_plan_seed,
    wait_for_kafka_match,
)
from .send_test_events import send_bin_telemetry_events, send_vehicle_location_events


@dataclass
class PipelineOutcome:
    passed: bool
    events_sent: str
    outputs_checked: str
    reason: str


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _fmt_bool(value: bool) -> str:
    return "PASS" if value else "FAIL"


def _build_zone_bin_specs(zone_seed: Dict[str, Any]) -> List[Dict[str, Any]]:
    bins: List[Dict[str, Any]] = list(zone_seed["bins"])
    fill_levels = [45.0, 82.0, 95.0, 68.0, 88.0]
    battery_levels = [91.0, 84.0, 80.0, 76.0, 72.0]

    specs: List[Dict[str, Any]] = []
    for index, row in enumerate(bins):
        if index >= 4:
            break
        specs.append(
            {
                "bin_id": row["bin_id"],
                "zone_id": row["zone_id"],
                "waste_category_id": row.get("waste_category_id"),
                "waste_category": row.get("waste_category") or None,
                "volume_litres": row.get("volume_litres"),
                "fill_level_pct": fill_levels[index % len(fill_levels)],
                "battery_level_pct": battery_levels[index % len(battery_levels)],
                "cluster_id": f"E2E-CLUSTER-{index + 1:03d}",
            }
        )
    return specs


def _build_report(results: Dict[str, PipelineOutcome]) -> None:
    print()
    print("Final verification report:")
    for pipeline_name in ("Pipeline 2", "Pipeline 3", "Pipeline 4"):
        outcome = results[pipeline_name]
        print(f"{pipeline_name}: {_fmt_bool(outcome.passed)}")
        print(f"  events sent: {outcome.events_sent}")
        print(f"  outputs checked: {outcome.outputs_checked}")
        print(f"  reason: {outcome.reason}")


def run_e2e_suite() -> Dict[str, PipelineOutcome]:
    settings = load_settings()
    test_run = str(uuid.uuid4())
    start_ts = _now_utc()

    zone_window_minutes = os.getenv("ZONE_WINDOW_MINUTES", "10")
    zone_slide_minutes = os.getenv("ZONE_SLIDE_MINUTES", "2")
    print(
        f"Pipeline 2 window mode: window_minutes={zone_window_minutes}, slide_minutes={zone_slide_minutes} "
        f"(set ZONE_WINDOW_MINUTES and ZONE_SLIDE_MINUTES for test-mode runs)"
    )

    results: Dict[str, PipelineOutcome] = {}

    zone_seed = get_bins_for_zone(settings, min_bins=3)
    route_seed = get_route_plan_seed(settings)
    zone_bin_specs: List[Dict[str, Any]] = []
    zone_id: Optional[int] = None
    bin_ids: List[str] = []

    if zone_seed is not None:
        zone_bin_specs = _build_zone_bin_specs(zone_seed)
        zone_id = int(zone_seed["zone_id"])
        bin_ids = [spec["bin_id"] for spec in zone_bin_specs]
    vehicle_id = route_seed["vehicle_id"] if route_seed else "LORRY-01"
    job_id = route_seed["job_id"] if route_seed else None
    route_waypoints = route_seed["waypoints"] if route_seed else None

    kafka_p1 = create_kafka_watcher(settings, settings.kafka_output_topic, start_from_latest=True)
    kafka_p2 = create_kafka_watcher(settings, settings.kafka_zone_output_topic, start_from_latest=True)
    kafka_p3 = create_kafka_watcher(settings, settings.kafka_vehicle_deviation_topic, start_from_latest=True)

    try:
        if zone_seed is not None and zone_id is not None and bin_ids:
            bin_events = send_bin_telemetry_events(test_run, zone_bin_specs, settings=settings, event_spacing_seconds=0.1)
            time.sleep(4)

            p1_kafka_ok, p1_kafka_reason, _ = wait_for_kafka_match(
                kafka_p1,
                lambda payload: isinstance(payload, dict)
                and payload.get("bin_id") in set(bin_ids)
                and isinstance(payload.get("status"), str)
                and isinstance(payload.get("event_ts"), str),
                timeout_s=45,
            )
            p1_pg_ok, p1_pg_reason = check_postgres_bin_state_updated(settings, bin_ids, since=start_ts)
            p1_influx_raw_ok = True
            p1_influx_raw_reason = "raw measurements matched"
            p1_influx_processed_ok = True
            p1_influx_processed_reason = "processed measurements matched"
            for bin_id in bin_ids:
                raw_ok, raw_reason = check_influx_for_measurement(
                    settings,
                    settings.influx_raw_bucket,
                    "bin_readings_raw",
                    tag_key="bin_id",
                    tag_value=str(bin_id),
                    timeout_s=30,
                )
                processed_ok, processed_reason = check_influx_for_measurement(
                    settings,
                    settings.influx_processed_bucket,
                    "bin_readings_processed",
                    tag_key="bin_id",
                    tag_value=str(bin_id),
                    timeout_s=30,
                )
                p1_influx_raw_ok = p1_influx_raw_ok and raw_ok
                p1_influx_processed_ok = p1_influx_processed_ok and processed_ok
                if not raw_ok:
                    p1_influx_raw_reason = raw_reason
                if not processed_ok:
                    p1_influx_processed_reason = processed_reason

            p1_ok = p1_kafka_ok and p1_pg_ok and p1_influx_raw_ok and p1_influx_processed_ok
            p1_reason = "; ".join(
                [
                    p1_kafka_reason,
                    p1_pg_reason,
                    p1_influx_raw_reason,
                    p1_influx_processed_reason,
                ]
            )
            results["Pipeline 1"] = PipelineOutcome(
                passed=p1_ok,
                events_sent=f"{len(bin_events)} bin telemetry events across zone_id={zone_id}",
                outputs_checked="Kafka waste.bin.processed, PostgreSQL f2.bin_current_state, InfluxDB bin_readings_raw/bin_readings_processed",
                reason=p1_reason,
            )

            p2_kafka_ok, p2_kafka_reason, _ = wait_for_kafka_match(
                kafka_p2,
                lambda payload: isinstance(payload, dict)
                and int(payload.get("zone_id", -1)) == zone_id
                and isinstance(payload.get("snapshot_at"), str)
                and payload.get("urgent_bin_count", 0) >= 1
                and payload.get("critical_bin_count", 0) >= 1,
                timeout_s=60,
            )
            p2_pg_ok, p2_pg_reason = check_postgres_zone_snapshot_since(settings, zone_id=zone_id, since=start_ts)
            p2_influx_ok, p2_influx_reason = check_influx_for_measurement(
                settings,
                settings.influx_zone_bucket,
                "zone_statistics",
                tag_key="zone_id",
                tag_value=str(zone_id),
                timeout_s=60,
            )
            p2_ok = p2_kafka_ok and p2_pg_ok and p2_influx_ok
            p2_reason = "; ".join([p2_kafka_reason, p2_pg_reason, p2_influx_reason])
            results["Pipeline 2"] = PipelineOutcome(
                passed=p2_ok,
                events_sent=f"{len(bin_events)} bin telemetry events for zone_id={zone_id} (multi-bin zone batch)",
                outputs_checked="Kafka waste.zone.statistics, PostgreSQL f2.zone_snapshots, InfluxDB zone_statistics",
                reason=p2_reason,
            )
        else:
            results["Pipeline 1"] = PipelineOutcome(
                passed=False,
                events_sent="0 bin telemetry events",
                outputs_checked="Kafka waste.bin.processed, PostgreSQL f2.bin_current_state, InfluxDB bin_readings_raw/bin_readings_processed",
                reason="No zone seed data was available to build the Pipeline 1 and 2 input batch",
            )
            results["Pipeline 2"] = PipelineOutcome(
                passed=False,
                events_sent="0 bin telemetry events",
                outputs_checked="Kafka waste.zone.statistics, PostgreSQL f2.zone_snapshots, InfluxDB zone_statistics",
                reason="No zone in f2.bins has enough active bins to trigger the sliding window test",
            )

        if route_seed is None:
            results["Pipeline 3"] = PipelineOutcome(
                passed=False,
                events_sent="0 vehicle deviation events",
                outputs_checked="Kafka waste.vehicle.deviation",
                reason="Route seed data is missing: route_plans table is unavailable or empty",
            )
        else:
            near_events = send_vehicle_location_events(
                test_run,
                vehicle_id,
                settings=settings,
                route_waypoints=route_waypoints,
                job_id=job_id,
                phase="near",
            )
            near_ok, near_reason, _ = wait_for_kafka_match(
                kafka_p3,
                lambda payload: isinstance(payload, dict)
                and payload.get("vehicle_id") == vehicle_id
                and isinstance(payload.get("deviation_m"), (int, float))
                and payload.get("deviation_m", 0) > 500,
                timeout_s=5,
            )
            far_events = send_vehicle_location_events(
                test_run,
                vehicle_id,
                settings=settings,
                route_waypoints=route_waypoints,
                job_id=job_id,
                phase="far",
            )
            p3_ok, p3_reason, p3_payload = check_kafka_alert_for_deviation(
                kafka_p3,
                vehicle_id=vehicle_id,
                job_id=job_id,
                timeout_s=60,
            )
            if near_ok:
                p3_ok = False
                p3_reason = "Alert appeared before the vehicle left the route"
            elif not p3_ok:
                p3_reason = p3_reason or near_reason

            results["Pipeline 3"] = PipelineOutcome(
                passed=p3_ok,
                events_sent=f"{len(near_events)} near-route GPS event(s) and {len(far_events)} off-route GPS event(s) for vehicle_id={vehicle_id}",
                outputs_checked="Kafka waste.vehicle.deviation",
                reason=p3_reason if p3_payload is None else f"{p3_reason}; alert={json.dumps(p3_payload, default=str)}",
            )

        vehicle_event_count = len(near_events if route_seed else []) + len(far_events if route_seed else [])
        if route_seed is not None:
            vehicle_influx_ok, vehicle_influx_reason = check_influx_for_measurement(
                settings,
                settings.influx_vehicle_bucket,
                "vehicle_positions",
                field_key="vehicle_id",
                field_value=vehicle_id,
                timeout_s=60,
            )
            results["Pipeline 4"] = PipelineOutcome(
                passed=vehicle_influx_ok,
                events_sent=f"{vehicle_event_count} GPS events for vehicle_id={vehicle_id}",
                outputs_checked="InfluxDB vehicle_positions",
                reason=vehicle_influx_reason,
            )
        else:
            results["Pipeline 4"] = PipelineOutcome(
                passed=False,
                events_sent="0 GPS events",
                outputs_checked="InfluxDB vehicle_positions",
                reason="Vehicle route seed data is missing, so the deviation/vehicle-position sequence was not executed",
            )

        return results
    finally:
        for consumer in (kafka_p1, kafka_p2, kafka_p3):
            try:
                consumer.close()
            except Exception:
                pass


def main() -> int:
    results = run_e2e_suite()
    _build_report(results)

    required = ["Pipeline 2", "Pipeline 3", "Pipeline 4"]
    return 0 if all(results[name].passed for name in required if name in results) else 1


def test_all_pipelines_e2e():
    assert main() == 0
