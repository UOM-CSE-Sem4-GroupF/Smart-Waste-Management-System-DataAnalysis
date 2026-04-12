# Smart Waste Management System - Data Analysis

Docker-first real-time waste analytics pipeline using MQTT, Kafka, Spark, PostgreSQL, and InfluxDB.

## Flow

1. `publisher.py` publishes waste bin events to MQTT.
2. `mqtt_to_kafka.py` forwards MQTT events into Kafka topic `waste-stream`.
3. `influxdb_consumer.py` writes stream data into InfluxDB.
4. `spark_stream.py` reads Kafka, computes predictions, and writes them into:
  - PostgreSQL prediction tables: `waste_predictions_5min`, `waste_predictions_4hour`, `waste_predictions_1day`, `waste_predictions_7day`
  - InfluxDB measurement: `waste_prediction`

## Prerequisites

1. Docker Desktop
2. Docker Compose
3. Git

## Clone and checkout Kalana branch

Use these commands from any folder:

```powershell
git clone https://github.com/UOM-CSE-Sem4-GroupF/Smart-Waste-Management-System-DataAnalysis.git
cd Smart-Waste-Management-System-DataAnalysis
git checkout Kalana
git fetch origin
```

## Run from scratch (Docker)

Build and start all services:

```powershell
docker-compose --profile apps up -d --build
```

Check container status:

```powershell
docker-compose --profile apps ps
```

## Observe runtime logs

```powershell
docker-compose logs -f publisher-app mqtt-to-kafka influxdb-consumer-app spark-stream-app
```

## Verify data in PostgreSQL

Raw stream row count(if no rows are in tables wait few minutes to let containers run and try again):

```powershell
docker exec -i postgres psql -U postgres -d waste_db -c "SELECT COUNT(*) AS raw_rows FROM waste_stream;"
```

Prediction row counts by horizon table(if no rows are in tables wait few minutes to let containers run and try again):

```powershell
docker exec -i postgres psql -U postgres -d waste_db -c "SELECT '5min' AS horizon, COUNT(*) AS rows FROM waste_predictions_5min UNION ALL SELECT '4hour', COUNT(*) FROM waste_predictions_4hour UNION ALL SELECT '1day', COUNT(*) FROM waste_predictions_1day UNION ALL SELECT '7day', COUNT(*) FROM waste_predictions_7day;"
```

Latest rows from each prediction table(if no rows are in tables wait few minutes to let containers run and try again):

```powershell
docker exec -i postgres psql -U postgres -d waste_db -c "SELECT * FROM waste_predictions_5min ORDER BY id DESC LIMIT 10;"
docker exec -i postgres psql -U postgres -d waste_db -c "SELECT * FROM waste_predictions_4hour ORDER BY id DESC LIMIT 10;"
docker exec -i postgres psql -U postgres -d waste_db -c "SELECT * FROM waste_predictions_1day ORDER BY id DESC LIMIT 10;"
docker exec -i postgres psql -U postgres -d waste_db -c "SELECT * FROM waste_predictions_7day ORDER BY id DESC LIMIT 10;"
```

## Verify data in InfluxDB

```powershell
docker exec -i influxdb influx query "from(bucket: \"waste-data\") |> range(start: -10m) |> filter(fn: (r) => r._measurement == \"waste_prediction\") |> limit(n: 20)"
```

View raw waste events from InfluxDB:

```powershell
docker exec -i influxdb influx query "from(bucket: \"waste-data\") |> range(start: -10m) |> filter(fn: (r) => r._measurement == \"waste\") |> limit(n: 20)"
```

View InfluxDB bucket list:

```powershell
docker exec -i influxdb influx bucket list
```

Optional InfluxDB Web UI:

1. Open `http://127.0.0.1:8086`
2. Login with below credintials
      Username: admin
      Password: password
3. Go to `Data Explorer`
4. Select bucket `waste-data`
5. Query measurements `waste` and `waste_prediction`

## pgAdmin UI

Open:

1. http://127.0.0.1:5050

Login credentials:

1. Email: `admin@example.com`
2. Password: `admin123`

Register server inside pgAdmin:

1. Right click `Servers` -> `Register` -> `Server`
2. General -> Name: `waste-db`
3. Connection tab:
  - Host: `postgres`
  - Port: `5432`
  - Maintenance DB: `waste_db`
  - Username: `postgres`
  - Password: value from `.env` (`POSTGRES_PASSWORD`)
4. Save

Open prediction tables:

1. `Servers` -> `waste-db` -> `Databases` -> `waste_db` -> `Schemas` -> `public` -> `Tables`
2. Right click a table -> `View/Edit Data` -> `All Rows`

## Stop and reset

Stop containers:

```powershell
docker-compose down
```

Stop and delete volumes (full reset):

```powershell
docker-compose down -v
```

## Commit and push to Kalana branch

```powershell
git add .
git commit -m "Project cleanup and Docker runbook README"
git push origin Kalana
```

## Project structure

```text
Smart-Waste-Management-System-DataAnalysis/
|-- docker-compose.yml
|-- Dockerfile.python
|-- Dockerfile.spark
|-- .env
|-- .env.example
|-- .gitignore
|-- README.md
|-- mosquitto.conf
|-- init.sql
|-- config.py
|-- publisher.py
|-- mqtt_to_kafka.py
|-- influxdb_consumer.py
|-- spark_stream.py
|-- requirements.docker.txt
`-- requirements.spark.txt
```

## What happens in the project

1. `publisher.py` generates sample waste bin readings.
2. Messages are published to MQTT topics (`waste/<bin_id>`).
3. `mqtt_to_kafka.py` consumes MQTT and pushes JSON to Kafka topic `waste-stream`.
4. `influxdb_consumer.py` consumes Kafka and writes raw stream points to InfluxDB measurement `waste`.
5. `spark_stream.py` reads Kafka with Spark Structured Streaming.
6. Spark computes prediction horizons (5 min, 4 hour, 1 day, 7 day).
7. Spark writes predictions to PostgreSQL tables:
  - `waste_predictions_5min`
  - `waste_predictions_4hour`
  - `waste_predictions_1day`
  - `waste_predictions_7day`
8. Spark also writes prediction points to InfluxDB measurement `waste_prediction`.

## End-to-end data flow

1. Sensor simulation: `publisher.py` -> MQTT (`mosquitto`)
2. Stream bridge: MQTT -> `mqtt_to_kafka.py` -> Kafka (`waste-stream`)
3. Raw storage path: Kafka -> `influxdb_consumer.py` -> InfluxDB (`waste`)
4. Analytics path: Kafka -> `spark_stream.py` -> PostgreSQL prediction tables
5. Time-series prediction path: `spark_stream.py` -> InfluxDB (`waste_prediction`)
