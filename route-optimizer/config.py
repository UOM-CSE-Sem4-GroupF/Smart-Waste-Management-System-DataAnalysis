from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Mapping


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str
    kafka_input_topic: str
    kafka_output_topic: str
    kafka_group_id: str
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str
    urgency_threshold: int

    @property
    def database_dsn(self) -> str:
        return (
            f"host={self.db_host} port={self.db_port} dbname={self.db_name} "
            f"user={self.db_user} password={self.db_password}"
        )


def load_settings(env: Mapping[str, str] | None = None) -> Settings:
    values = env or os.environ
    return Settings(
        kafka_bootstrap_servers=values.get("KAFKA_BROKERS", values.get("KAFKA_BROKER", "localhost:9092")),
        kafka_input_topic=values.get("KAFKA_INPUT_TOPIC", "waste.bin.processed"),
        kafka_output_topic=values.get("KAFKA_OUTPUT_TOPIC", "waste.routes.optimized"),
        kafka_group_id=values.get("KAFKA_GROUP_ID", "route-optimizer"),
        db_host=values.get("DB_HOST", "localhost"),
        db_port=int(values.get("DB_PORT", "5432")),
        db_name=values.get("DB_NAME", "waste_management"),
        db_user=values.get("DB_USER", "waste_admin"),
        db_password=values.get("DB_PASSWORD", "waste_admin_password"),
        urgency_threshold=int(values.get("ROUTE_OPTIMIZER_URGENCY_THRESHOLD", "70")),
    )