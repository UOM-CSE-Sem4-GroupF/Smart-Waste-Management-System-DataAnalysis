from __future__ import annotations

from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    # No Kafka, no DB — this service is stateless.

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


def load_settings() -> Settings:
    return Settings()