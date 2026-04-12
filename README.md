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
```

If `Kalana` branch does not exist locally:

```powershell
git fetch origin
git checkout -b Kalana origin/Kalana
```

## Environment setup

Create `.env` from template:

```powershell
Copy-Item .env.example .env
```

Edit `.env` and set at least:

1. `POSTGRES_PASSWORD`
2. `INFLUXDB_TOKEN`
3. Prediction frequency variables (minutes):
  - `PRED_HORIZON_5MIN`
  - `PRED_HORIZON_4HOUR`
  - `PRED_HORIZON_1DAY`
  - `PRED_HORIZON_7DAY`

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

Raw stream row count:

```powershell
docker exec -i postgres psql -U postgres -d waste_db -c "SELECT COUNT(*) AS raw_rows FROM waste_stream;"
```

Prediction row counts by horizon table:

```powershell
docker exec -i postgres psql -U postgres -d waste_db -c "SELECT '5min' AS horizon, COUNT(*) AS rows FROM waste_predictions_5min UNION ALL SELECT '4hour', COUNT(*) FROM waste_predictions_4hour UNION ALL SELECT '1day', COUNT(*) FROM waste_predictions_1day UNION ALL SELECT '7day', COUNT(*) FROM waste_predictions_7day;"
```

Latest rows from each prediction table:

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
