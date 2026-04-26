from dataclasses import dataclass
from datetime import datetime, timezone
import os


@dataclass(frozen=True)
class Settings:
    service_name: str = "fastapi-ml-service"
    service_version: str = os.getenv("SERVICE_VERSION", "1.0.0")
    api_prefix: str = "/api/v1/ml"


SETTINGS = Settings()
LOADED_AT_UTC = datetime.now(timezone.utc)
