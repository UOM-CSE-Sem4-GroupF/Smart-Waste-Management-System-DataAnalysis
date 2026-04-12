"""
Configuration module for the Waste Management System.
Reads from environment variables for portability across deployments.
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# MQTT Configuration
MQTT_HOST = os.getenv('MQTT_HOST', 'localhost')
MQTT_PORT = int(os.getenv('MQTT_PORT', 1883))

# Kafka Configuration
KAFKA_HOST = os.getenv('KAFKA_HOST', 'localhost')
KAFKA_PORT = int(os.getenv('KAFKA_PORT', 9092))

# PostgreSQL Configuration
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = int(os.getenv('POSTGRES_PORT', 5432))
POSTGRES_DB = os.getenv('POSTGRES_DB', 'waste_db')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'yourpassword')

# InfluxDB Configuration
INFLUXDB_HOST = os.getenv('INFLUXDB_HOST', 'localhost')
INFLUXDB_PORT = int(os.getenv('INFLUXDB_PORT', 8086))
INFLUXDB_ORG = os.getenv('INFLUXDB_ORG', 'my-org')
INFLUXDB_BUCKET = os.getenv('INFLUXDB_BUCKET', 'waste-data')
INFLUXDB_TOKEN = os.getenv('INFLUXDB_TOKEN', '')

# Deployment Mode
DEPLOYMENT_MODE = os.getenv('DEPLOYMENT_MODE', 'local')

# Kafka Bootstrap String
KAFKA_BOOTSTRAP_SERVERS = f'{KAFKA_HOST}:{KAFKA_PORT}'

# PostgreSQL Connection String
POSTGRES_CONNECTION_STRING = f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}'

# InfluxDB URL
INFLUXDB_URL = f'http://{INFLUXDB_HOST}:{INFLUXDB_PORT}'

# Print configuration on startup (for debugging)
if __name__ == '__main__':
    print("Current Configuration:")
    print(f"Deployment Mode: {DEPLOYMENT_MODE}")
    print(f"MQTT: {MQTT_HOST}:{MQTT_PORT}")
    print(f"Kafka: {KAFKA_HOST}:{KAFKA_PORT}")
    print(f"PostgreSQL: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    print(f"InfluxDB: {INFLUXDB_HOST}:{INFLUXDB_PORT}")
