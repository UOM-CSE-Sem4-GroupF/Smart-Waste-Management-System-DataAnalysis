# Waste Management Streaming System

This project is a Docker-first real-time waste monitoring and prediction pipeline.
It publishes waste bin fill data, routes it through MQTT and Kafka, stores the raw stream, and writes horizon-based predictions into PostgreSQL and InfluxDB.

## What the stack does

- `publisher.py` generates synthetic bin fill events and publishes them to MQTT.
- `mqtt_to_kafka.py` bridges MQTT messages into Kafka.
- `influxdb_consumer.py` consumes Kafka and stores the stream in InfluxDB.
- `spark_stream.py` reads Kafka, builds real-time waste predictions, and stores them in PostgreSQL and InfluxDB.
- `docker-compose.yml` brings up the entire stack, including pgAdmin.

## Repository layout

- `docker-compose.yml` - orchestrates all services
- `Dockerfile.python` - image for the Python app containers
- `Dockerfile.spark` - custom Spark image with the prediction dependencies
- `publisher.py` - MQTT data producer
- `mqtt_to_kafka.py` - MQTT to Kafka bridge
- `influxdb_consumer.py` - Kafka to InfluxDB consumer
- `spark_stream.py` - real-time prediction pipeline
- `config.py` - shared environment-based configuration
- `init.sql` - PostgreSQL schema initialization
- `mosquitto.conf` - MQTT broker config
- `requirements.docker.txt` - Python dependencies for the app containers
- `requirements.spark.txt` - Python dependencies for the Spark container
- `.env.example` - example environment file to copy to `.env`

## Prerequisites

- Docker Desktop installed and running
- Docker Compose available in your terminal
- Git, if you want to clone the repository

## Setup from scratch on Windows

1. Clone the repository.
2. Copy the example environment file to `.env`.

```powershell
Copy-Item .env.example .env
```

3. Open `.env` and set the values you want to use.
   - Keep `INFLUXDB_TOKEN` consistent with the InfluxDB setup value.
   - If you want to change prediction timing, edit these four variables in `.env`:
     - `PRED_HORIZON_5MIN`
     - `PRED_HORIZON_4HOUR`
     - `PRED_HORIZON_1DAY`
     - `PRED_HORIZON_7DAY`

4. Start the full stack.

```powershell
docker-compose --profile apps up -d --build
```

## How to observe the project

### Live logs

```powershell
docker-compose logs -f publisher-app mqtt-to-kafka influxdb-consumer-app spark-stream-app
```

Watch for messages such as:
- `Data sent...` from `publisher-app`
- `Sent to kafka...` from `mqtt-to-kafka`
- `Data written to InfluxDB...` from `influxdb-consumer-app`
- `Batch ... prediction rows written...` from `spark-stream-app`

### PostgreSQL raw stream

```powershell
docker exec -i postgres psql -U postgres -d waste_db -c "SELECT COUNT(*) FROM waste_stream;"
```

### PostgreSQL prediction tables

```powershell
docker exec -i postgres psql -U postgres -d waste_db -c "SELECT '5min' AS horizon, COUNT(*) AS rows FROM waste_predictions_5min UNION ALL SELECT '4hour', COUNT(*) FROM waste_predictions_4hour UNION ALL SELECT '1day', COUNT(*) FROM waste_predictions_1day UNION ALL SELECT '7day', COUNT(*) FROM waste_predictions_7day;"
```

### Latest prediction rows

```powershell
docker exec -i postgres psql -U postgres -d waste_db -c "SELECT bin_id, horizon_label, predicted_fill, confidence, model_version, predicted_at FROM waste_predictions_5min ORDER BY id DESC LIMIT 10;"
```

Repeat the query for `waste_predictions_4hour`, `waste_predictions_1day`, or `waste_predictions_7day`.

## pgAdmin UI

pgAdmin is included in the compose file.
Open:

- [http://127.0.0.1:5050](http://127.0.0.1:5050)

Login:
- Email: `admin@example.com`
- Password: `admin123`

Then register the PostgreSQL server:
- Host name/address: `postgres`
- Port: `5432`
- Maintenance database: `waste_db`
- Username: `postgres`
- Password: `yourpassword`

After that, expand:
- Servers
- Your registered server
- Databases
- `waste_db`
- Schemas
- `public`
- Tables

You will see:
- `waste_stream`
- `waste_predictions_5min`
- `waste_predictions_4hour`
- `waste_predictions_1day`
- `waste_predictions_7day`
- `model_metrics`

## Stopping the stack

```powershell
docker-compose down
```

If you also want to remove volumes and start fresh:

```powershell
docker-compose down -v
```

## Notes

- The project is designed to run fully in Docker.
- `spark_stream.py` uses a real-time trend-based predictor and writes one prediction set per bin to four separate PostgreSQL tables.
- If you change the horizon values, restart the Spark service so the new values are picked up.
