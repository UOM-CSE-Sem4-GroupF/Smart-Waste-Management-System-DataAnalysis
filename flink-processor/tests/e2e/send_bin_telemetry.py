import argparse
import json
import logging
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

from kafka import KafkaProducer


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import load_settings  # noqa: E402


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("e2e-send-bin-telemetry")


def _build_producer(settings) -> KafkaProducer:
    config = {
        "bootstrap_servers": settings.kafka_bootstrap_servers,
        "value_serializer": lambda value: json.dumps(value, default=str).encode("utf-8"),
    }
    if settings.kafka_username and settings.kafka_password:
        config.update(
            {
                "security_protocol": settings.kafka_security_protocol,
                "sasl_mechanism": settings.kafka_sasl_mechanism,
                "sasl_plain_username": settings.kafka_username,
                "sasl_plain_password": settings.kafka_password,
            }
        )
    return KafkaProducer(**config)


def _msg(bin_id: str, ts: datetime, fill: float, battery: float, **extra: Any) -> Dict[str, Any]:
    payload = {
        "bin_id": bin_id,
        "fill_level_pct": fill,
        "battery_level_pct": battery,
        "timestamp": ts.isoformat().replace("+00:00", "Z"),
    }
    payload.update(extra)
    return {"payload": payload}


def build_scenarios(base_time: datetime) -> List[Dict[str, Any]]:
    return [
        {
            "name": "normal bin",
            "event": _msg("BIN-001", base_time + timedelta(seconds=0), 35.0, 92.0, signal_strength=-75, temperature_c=28.0),
        },
        {
            "name": "monitor bin",
            "event": _msg("BIN-002", base_time + timedelta(seconds=10), 60.0, 90.0, signal_strength=-78, temperature_c=27.0),
        },
        {
            "name": "urgent bin",
            "event": _msg("BIN-003", base_time + timedelta(seconds=20), 82.0, 88.0, signal_strength=-80, temperature_c=29.0),
        },
        {
            "name": "critical bin",
            "event": _msg("BIN-004", base_time + timedelta(seconds=30), 95.0, 86.0, signal_strength=-81, temperature_c=30.0),
        },
        {
            "name": "low battery bin",
            "event": _msg("BIN-005", base_time + timedelta(seconds=40), 68.0, 12.0, signal_strength=-79, temperature_c=27.0),
        },
        {
            "name": "rapid filling start",
            "event": _msg("BIN-006", base_time + timedelta(seconds=50), 40.0, 89.0, signal_strength=-77, temperature_c=26.5),
        },
        {
            "name": "rapid filling spike",
            "event": _msg("BIN-006", base_time + timedelta(seconds=80), 88.0, 88.0, signal_strength=-76, temperature_c=27.0),
        },
        {
            "name": "possible tampering scenario",
            "event": _msg("BIN-007", base_time + timedelta(seconds=90), 74.0, 85.0, signal_strength=-118, temperature_c=91.0),
        },
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description="Send E2E bin telemetry test events")
    parser.add_argument("--sleep-seconds", type=float, default=0.2, help="Delay between sends")
    args = parser.parse_args()

    settings = load_settings()
    producer = _build_producer(settings)
    topic = settings.kafka_input_topic

    base_time = datetime.now(timezone.utc)
    scenarios = build_scenarios(base_time)

    logger.info("Sending %d bin telemetry scenarios to topic=%s", len(scenarios), topic)

    try:
        for item in scenarios:
            future = producer.send(topic, item["event"])
            result = future.get(timeout=10)
            payload = item["event"]["payload"]
            logger.info(
                "Sent %-26s bin_id=%s fill=%.1f battery=%.1f offset=%s",
                item["name"],
                payload["bin_id"],
                float(payload["fill_level_pct"]),
                float(payload["battery_level_pct"]),
                result.offset,
            )
            if args.sleep_seconds > 0:
                time.sleep(args.sleep_seconds)
    finally:
        producer.flush()
        producer.close()

    logger.info("Completed sending bin telemetry E2E events")


if __name__ == "__main__":
    main()
