from __future__ import annotations

import json
import logging
from typing import Any

from config import load_settings
from repository import RouteOptimizerRepository
from service import prepare_emergency_run
from solver import solve_emergency_routes


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("route-optimizer")


def create_connection(settings):
    from psycopg2 import connect

    return connect(settings.database_dsn)


def create_repository(settings):
    return RouteOptimizerRepository(lambda: create_connection(settings))


def create_consumer(settings):
    from kafka import KafkaConsumer

    return KafkaConsumer(
        settings.kafka_input_topic,
        bootstrap_servers=[settings.kafka_bootstrap_servers],
        group_id=settings.kafka_group_id,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )


def health() -> dict[str, Any]:
    return {"status": "ok", "service": "route-optimizer", "version": "stage2"}


def run() -> None:
    settings = load_settings()
    repository = create_repository(settings)
    consumer = create_consumer(settings)

    logger.info("route optimizer ready; waiting for urgent bin events")
    for message in consumer:
        event = message.value
        try:
            result = prepare_emergency_run(event, repository, settings)
            if result is None:
                logger.info("skipping non-urgent event for bin %s", event.get("payload", {}).get("bin_id"))
                continue

            snapshot = result.snapshot
            plan = solve_emergency_routes(snapshot)
            logger.info(
                "optimized zone=%s solver=%s urgent_bins=%s vehicles=%s routes=%s unassigned=%s total_weight=%.2f",
                snapshot.zone_id,
                plan.solver_used,
                result.urgent_bins_count,
                result.vehicle_count,
                len(plan.routes),
                len(plan.unassigned_bins),
                plan.total_weight_kg,
            )
        except Exception as exc:
            logger.exception("failed to prepare optimization input: %s", exc)


if __name__ == "__main__":
    run()