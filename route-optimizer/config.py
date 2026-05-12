from __future__ import annotations

from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    osrm_server_url: str = Field(
        default="https://router.project-osrm.org", env="OSRM_SERVER_URL"
    )

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


def load_settings() -> Settings:
    return Settings()