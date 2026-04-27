from __future__ import annotations

from collections import deque
import json
import logging
from typing import Any

from config import load_settings
from repository import RouteOptimizerRepository
from service import (
    build_deterministic_job_id,
    build_optimized_route_event,
    persist_optimization_plan,
    prepare_emergency_run,
    publish_optimized_route_event,
)
from solver import solve_emergency_routes


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("route-optimizer")

MAX_PROCESSED_JOB_CACHE = 50_000


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
        enable_auto_commit=False,
    )


def create_producer(settings):
    from kafka import KafkaProducer

    return KafkaProducer(
        bootstrap_servers=[settings.kafka_bootstrap_servers],
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def health() -> dict[str, Any]:
    return {"status": "ok", "service": "route-optimizer", "version": "stage3"}


def _track_processed_job(
    job_id: str,
    processed_job_ids: set[str],
    processed_job_order: deque[str],
) -> None:
    if job_id in processed_job_ids:
        return

    processed_job_ids.add(job_id)
    processed_job_order.append(job_id)
    if len(processed_job_order) > MAX_PROCESSED_JOB_CACHE:
        evicted = processed_job_order.popleft()
        processed_job_ids.discard(evicted)


def handle_event(
    event: dict[str, Any],
    repository: RouteOptimizerRepository,
    producer: Any,
    settings: Any,
    processed_job_ids: set[str],
    processed_job_order: deque[str],
) -> bool:
    result = prepare_emergency_run(event, repository, settings)
    if result is None:
        logger.info("skipping non-urgent event for bin %s", event.get("payload", {}).get("bin_id"))
        return True

    snapshot = result.snapshot
    plan = solve_emergency_routes(snapshot)
    job_id = build_deterministic_job_id(snapshot)
    if job_id in processed_job_ids:
        logger.info("skipping duplicate event for job_id=%s", job_id)
        return True

    persistence = persist_optimization_plan(repository, snapshot, plan, job_id)
    if persistence.already_exists:
        _track_processed_job(job_id, processed_job_ids, processed_job_order)
        logger.info("route plan already persisted for job_id=%s; skipping publish", job_id)
        return True

    output_event = build_optimized_route_event(snapshot, plan, job_id)
    publish_optimized_route_event(producer, settings.kafka_output_topic, output_event)
    _track_processed_job(job_id, processed_job_ids, processed_job_order)
    logger.info(
        "optimized zone=%s job_id=%s solver=%s urgent_bins=%s vehicles=%s routes=%s unassigned=%s inserted_rows=%s total_weight=%.2f",
        snapshot.zone_id,
        job_id,
        plan.solver_used,
        result.urgent_bins_count,
        result.vehicle_count,
        len(plan.routes),
        len(plan.unassigned_bins),
        persistence.inserted_rows,
        plan.total_weight_kg,
    )
    return True


def run() -> None:
    settings = load_settings()
    repository = create_repository(settings)
    consumer = create_consumer(settings)
    producer = create_producer(settings)
    processed_job_ids: set[str] = set()
    processed_job_order: deque[str] = deque()

    logger.info("route optimizer ready; waiting for urgent bin events")
    for message in consumer:
        event = message.value
        try:
            should_commit = handle_event(
                event,
                repository,
                producer,
                settings,
                processed_job_ids,
                processed_job_order,
            )
            if should_commit:
                consumer.commit()
        except Exception as exc:
            logger.exception("failed to prepare optimization input: %s", exc)


if __name__ == "__main__":
    run()