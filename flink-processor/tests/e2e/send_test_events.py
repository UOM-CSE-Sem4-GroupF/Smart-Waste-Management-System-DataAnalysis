import argparse
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Union

from kafka import KafkaProducer

try:
    from ...config import load_settings
except ImportError:
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))
    from config import load_settings


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("e2e-send-test-events")


def _get_kafka_producer(settings) -> KafkaProducer:
    broker = os.getenv("KAFKA_BROKER") or settings.kafka_bootstrap_servers
    username = os.getenv("KAFKA_USER") or settings.kafka_username
    password = os.getenv("KAFKA_PASS") or settings.kafka_password

    if not broker:
        raise RuntimeError("Missing Kafka broker address. Set KAFKA_BROKER or define it in .env")

    kwargs = {"bootstrap_servers": broker.split(",")}
    if username and password:
        kwargs.update(
            {
                "security_protocol": settings.kafka_security_protocol or "SASL_PLAINTEXT",
                "sasl_mechanism": settings.kafka_sasl_mechanism or "SCRAM-SHA-256",
                "sasl_plain_username": username,
                "sasl_plain_password": password,
            }
        )

    return KafkaProducer(
        **kwargs,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        retries=5,
    )


def _isoformat_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _build_bin_payload(
    *,
    bin_id: str,
    test_run_id: str,
    timestamp: datetime,
    fill_level_pct: float,
    battery_level_pct: float,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "bin_id": bin_id,
        "timestamp": _isoformat_utc(timestamp),
        "test_run_id": test_run_id,
        "fill_level_pct": fill_level_pct,
        "battery_level_pct": battery_level_pct,
    }
    if extra:
        payload.update(extra)
    return {"payload": payload}


def _build_default_bin_variants(bin_id: str, test_run_id: str, base_time: datetime) -> List[Dict[str, Any]]:
    return [
        _build_bin_payload(
            bin_id=bin_id,
            test_run_id=test_run_id,
            timestamp=base_time + timedelta(seconds=0),
            fill_level_pct=35.0,
            battery_level_pct=92.0,
            extra={"signal_strength": -75, "temperature_c": 28.0},
        ),
        _build_bin_payload(
            bin_id=bin_id,
            test_run_id=test_run_id,
            timestamp=base_time + timedelta(seconds=10),
            fill_level_pct=60.0,
            battery_level_pct=90.0,
            extra={"signal_strength": -78, "temperature_c": 27.0},
        ),
        _build_bin_payload(
            bin_id=bin_id,
            test_run_id=test_run_id,
            timestamp=base_time + timedelta(seconds=20),
            fill_level_pct=82.0,
            battery_level_pct=88.0,
            extra={"signal_strength": -80, "temperature_c": 29.0},
        ),
        _build_bin_payload(
            bin_id=bin_id,
            test_run_id=test_run_id,
            timestamp=base_time + timedelta(seconds=30),
            fill_level_pct=95.0,
            battery_level_pct=86.0,
            extra={"signal_strength": -81, "temperature_c": 30.0},
        ),
        _build_bin_payload(
            bin_id=bin_id,
            test_run_id=test_run_id,
            timestamp=base_time + timedelta(seconds=40),
            fill_level_pct=68.0,
            battery_level_pct=12.0,
            extra={"signal_strength": -79, "temperature_c": 27.0},
        ),
        _build_bin_payload(
            bin_id=bin_id,
            test_run_id=test_run_id,
            timestamp=base_time + timedelta(seconds=50),
            fill_level_pct=40.0,
            battery_level_pct=89.0,
            extra={"signal_strength": -77, "temperature_c": 26.5, "fill_rate_pct_per_hour": 20.0},
        ),
        _build_bin_payload(
            bin_id=bin_id,
            test_run_id=test_run_id,
            timestamp=base_time + timedelta(seconds=80),
            fill_level_pct=74.0,
            battery_level_pct=85.0,
            extra={"signal_strength": -118, "temperature_c": 91.0},
        ),
    ]


def _build_zone_batch_events(
    test_run_id: str,
    bin_specs: Sequence[Dict[str, Any]],
    base_time: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    if base_time is None:
        base_time = datetime.now(timezone.utc)

    fill_levels = [45.0, 78.0, 95.0, 62.0, 88.0, 72.0]
    battery_levels = [91.0, 84.0, 80.0, 73.0, 69.0, 65.0]
    events: List[Dict[str, Any]] = []

    for index, spec in enumerate(bin_specs):
        ts = base_time + timedelta(seconds=index * 20)
        extra: Dict[str, Any] = {
            "zone_id": spec.get("zone_id"),
            "waste_category_id": spec.get("waste_category_id"),
            "waste_category": spec.get("waste_category"),
            "cluster_id": spec.get("cluster_id"),
            "volume_litres": spec.get("volume_litres"),
            "test_run_id": test_run_id,
        }
        if spec.get("fill_rate_pct_per_hour") is not None:
            extra["fill_rate_pct_per_hour"] = spec["fill_rate_pct_per_hour"]

        event = _build_bin_payload(
            bin_id=str(spec["bin_id"]),
            test_run_id=test_run_id,
            timestamp=ts,
            fill_level_pct=float(spec.get("fill_level_pct", fill_levels[index % len(fill_levels)])),
            battery_level_pct=float(spec.get("battery_level_pct", battery_levels[index % len(battery_levels)])),
            extra=extra,
        )
        events.append(event)

    return events


def send_bin_telemetry_events(
    test_run_id: str,
    bin_spec_or_events: Union[str, Sequence[Dict[str, Any]], None],
    settings=None,
    *,
    event_spacing_seconds: float = 0.2,
) -> List[Dict[str, Any]]:
    if settings is None:
        settings = load_settings()

    topic = os.getenv("KAFKA_TOPIC") or settings.kafka_input_topic
    producer = _get_kafka_producer(settings)

    base_time = datetime.now(timezone.utc)
    if isinstance(bin_spec_or_events, str) or bin_spec_or_events is None:
        bin_id = bin_spec_or_events or os.getenv("TEST_BIN_ID") or "BIN-TEST-001"
        events = _build_default_bin_variants(bin_id, test_run_id, base_time)
    else:
        events = _build_zone_batch_events(test_run_id, bin_spec_or_events, base_time=base_time)

    logger.info("Sending %d bin telemetry events to topic=%s", len(events), topic)

    try:
        for index, message in enumerate(events):
            future = producer.send(topic, value=message)
            result = future.get(timeout=10)
            payload = message["payload"]
            logger.info(
                "Sent bin_id=%s fill=%.1f battery=%.1f offset=%s",
                payload["bin_id"],
                float(payload["fill_level_pct"]),
                float(payload["battery_level_pct"]),
                result.offset,
            )
            if event_spacing_seconds > 0 and index < len(events) - 1:
                time.sleep(event_spacing_seconds)
    finally:
        producer.flush()
        producer.close()

    return events


def _build_vehicle_event(
    *,
    vehicle_id: str,
    timestamp: datetime,
    latitude: float,
    longitude: float,
    test_run_id: str,
    job_id: Optional[str] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "vehicle_id": vehicle_id,
        "latitude": latitude,
        "longitude": longitude,
        "timestamp": _isoformat_utc(timestamp),
        "test_run_id": test_run_id,
    }
    if job_id:
        payload["job_id"] = job_id
    return {"payload": payload}


def _near_point(route_waypoints: Optional[Sequence[Dict[str, Any]]]) -> Dict[str, float]:
    if route_waypoints:
        waypoint = route_waypoints[0]
        latitude = waypoint.get("latitude", waypoint.get("lat"))
        longitude = waypoint.get("longitude", waypoint.get("lng", waypoint.get("lon")))
        if isinstance(latitude, (int, float)) and isinstance(longitude, (int, float)):
            return {"latitude": float(latitude) + 0.0001, "longitude": float(longitude) + 0.0001}
    return {"latitude": 6.9271, "longitude": 79.8612}


def send_vehicle_location_events(
    test_run_id: str,
    vehicle_id: str,
    settings=None,
    route_waypoints: Optional[Sequence[Dict[str, Any]]] = None,
    job_id: Optional[str] = None,
    phase: str = "all",
    event_spacing_seconds: float = 0.2,
) -> List[Dict[str, Any]]:
    if settings is None:
        settings = load_settings()

    topic = os.getenv("KAFKA_TOPIC_VEHICLE") or settings.kafka_vehicle_location_topic
    producer = _get_kafka_producer(settings)
    now = datetime.now(timezone.utc)

    near = _near_point(route_waypoints)
    far = {"latitude": near["latitude"] + 0.02, "longitude": near["longitude"] + 0.02}

    event_plan: List[Dict[str, Any]] = []
    if phase in {"near", "all"}:
        event_plan.append(
            _build_vehicle_event(
                vehicle_id=vehicle_id,
                timestamp=now,
                latitude=near["latitude"],
                longitude=near["longitude"],
                test_run_id=test_run_id,
                job_id=job_id,
            )
        )
    if phase in {"far", "all"}:
        event_plan.append(
            _build_vehicle_event(
                vehicle_id=vehicle_id,
                timestamp=now + timedelta(seconds=30),
                latitude=far["latitude"],
                longitude=far["longitude"],
                test_run_id=test_run_id,
                job_id=job_id,
            )
        )
        event_plan.append(
            _build_vehicle_event(
                vehicle_id=vehicle_id,
                timestamp=now + timedelta(seconds=151),
                latitude=far["latitude"] + 0.0005,
                longitude=far["longitude"] + 0.0005,
                test_run_id=test_run_id,
                job_id=job_id,
            )
        )

    logger.info("Sending %d vehicle location events to topic=%s", len(event_plan), topic)

    try:
        for index, message in enumerate(event_plan):
            future = producer.send(topic, value=message)
            result = future.get(timeout=10)
            payload = message["payload"]
            logger.info(
                "Sent vehicle_id=%s lat=%.5f lon=%.5f ts=%s offset=%s",
                payload["vehicle_id"],
                float(payload["latitude"]),
                float(payload["longitude"]),
                payload["timestamp"],
                result.offset,
            )
            if event_spacing_seconds > 0 and index < len(event_plan) - 1:
                time.sleep(event_spacing_seconds)
    finally:
        producer.flush()
        producer.close()

    return event_plan


def main() -> int:
    parser = argparse.ArgumentParser(description="Send E2E test events for all pipelines")
    parser.add_argument("--bin-id", default=os.getenv("TEST_BIN_ID") or "BIN-TEST-001")
    parser.add_argument("--vehicle-id", default=os.getenv("TEST_VEHICLE_ID") or "VEHICLE-TEST-001")
    parser.add_argument("--phase", choices=["all", "near", "far"], default="all")
    args = parser.parse_args()

    settings = load_settings()
    test_run_id = str(uuid.uuid4())

    logger.info("Sending test events test_run_id=%s", test_run_id)
    send_bin_telemetry_events(test_run_id, args.bin_id, settings=settings)
    send_vehicle_location_events(test_run_id, args.vehicle_id, settings=settings, phase=args.phase)
    logger.info("Completed sending test events")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
