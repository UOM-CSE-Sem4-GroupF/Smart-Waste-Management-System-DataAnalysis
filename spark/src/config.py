import os

# Database Configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres-waste")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "waste_management")
POSTGRES_USER = os.getenv("POSTGRES_USER", "waste_admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "waste_admin_password")

JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
JDBC_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}

# MLflow Configuration
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
EXPERIMENT_NAME = "waste-model-training"

# Model Names
MODEL_FILL_TIME = "waste-fill-time-model"
MODEL_ZONE_GEN = "waste-zone-generation-model"
MODEL_ROUTE_SCORE = "waste-route-score-model"
