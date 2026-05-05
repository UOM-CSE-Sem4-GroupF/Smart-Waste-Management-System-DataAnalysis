import os
from dataclasses import dataclass
from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class Settings:
    # Environment & logging
    app_env: str
    log_level: str
    processing_timezone: str

    # Kafka connectivity
    kafka_bootstrap_servers: str
    kafka_input_topic: str
    kafka_vehicle_location_topic: str
    kafka_vehicle_deviation_topic: str
    kafka_output_topic: str
    kafka_zone_input_topic: str
    kafka_zone_output_topic: str
    kafka_security_protocol: str
    kafka_sasl_mechanism: str
    kafka_username: str
    kafka_password: str

    # PostgreSQL connectivity (F2 schema)
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str
    postgres_schema: str

    # InfluxDB connectivity
    influx_url: str
    influx_org: str
    influx_token: str
    influx_enabled: bool
    influx_raw_bucket: str
    influx_processed_bucket: str
    influx_zone_bucket: str
    influx_vehicle_bucket: str

    # Processing mode
    flink_input_mode: str
    local_test_input_file: str
    local_zone_test_input_file: str

    # Zone aggregation settings
    zone_window_minutes: int
    zone_slide_minutes: int



def _get_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _get_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}



def load_settings() -> Settings:
    return Settings(
        app_env=os.getenv("APP_ENV", "dev"),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        kafka_input_topic=os.getenv("KAFKA_INPUT_TOPIC", "waste.bin.telemetry"),
        kafka_vehicle_location_topic=os.getenv("KAFKA_VEHICLE_LOCATION_TOPIC", "waste.vehicle.location"),
        kafka_vehicle_deviation_topic=os.getenv(
            "KAFKA_VEHICLE_DEVIATION_TOPIC",
            "waste.vehicle.deviation",
        ),
        kafka_output_topic=os.getenv("KAFKA_OUTPUT_TOPIC", "waste.bin.processed"),
        kafka_zone_input_topic=os.getenv("KAFKA_ZONE_INPUT_TOPIC", "waste.bin.processed"),
        kafka_zone_output_topic=os.getenv("KAFKA_ZONE_OUTPUT_TOPIC", "waste.zone.statistics"),
        kafka_security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT"),
        kafka_sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256"),
        kafka_username=os.getenv("KAFKA_USERNAME", ""),
        kafka_password=os.getenv("KAFKA_PASSWORD", ""),
        postgres_host=os.getenv("POSTGRES_HOST", "localhost"),
        postgres_port=_get_int("POSTGRES_PORT", 5432),
        postgres_db=os.getenv("POSTGRES_DB", "waste_management"),
        postgres_user=os.getenv("POSTGRES_USER", "waste_admin"),
        postgres_password=os.getenv("POSTGRES_PASSWORD", "waste_admin_password"),
        postgres_schema=os.getenv("POSTGRES_SCHEMA", "f2"),
        influx_url=os.getenv("INFLUX_URL", "http://localhost:8086"),
        influx_org=os.getenv("INFLUX_ORG", "waste-org"),
        influx_token=os.getenv("INFLUX_TOKEN", "my-super-token"),
        influx_enabled=_get_bool("INFLUX_ENABLED", True),
        influx_raw_bucket=os.getenv("INFLUX_RAW_BUCKET", "bin_readings_raw"),
        influx_processed_bucket=os.getenv("INFLUX_PROCESSED_BUCKET", "bin_readings_processed"),
        influx_zone_bucket=os.getenv("INFLUX_ZONE_BUCKET", "zone_statistics"),
        influx_vehicle_bucket=os.getenv("INFLUX_VEHICLE_BUCKET", "vehicle_positions"),
        flink_input_mode=os.getenv("FLINK_INPUT_MODE", "kafka"),
        processing_timezone=os.getenv("PROCESSING_TIMEZONE", "UTC"),
        local_test_input_file=os.getenv("LOCAL_TEST_INPUT_FILE", "./tests/data/bin_telemetry_sample.jsonl"),
        local_zone_test_input_file=os.getenv(
            "LOCAL_ZONE_TEST_INPUT_FILE",
            "./tests/data/processed_bin_sample.jsonl",
        ),
        zone_window_minutes=_get_int("ZONE_WINDOW_MINUTES", 10),
        zone_slide_minutes=_get_int("ZONE_SLIDE_MINUTES", 2),
    )
