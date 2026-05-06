# 🚀 SWMS Data Analysis Pipeline Guide

This guide contains the commands required to start and monitor the telemetry processing pipeline (Pipeline 1).

---

## 1. Start the Database Stack
This includes PostgreSQL (metadata & state) and InfluxDB (time-series).

```powershell
# Navigate to the db directory
cd db

# Start the database services
# -d runs in detached mode
docker compose up -d
```

> [!TIP]
> **To re-initialize the database (clean start):**
> If you change the schema or seed file and want to reset the database:
> `docker compose down -v ; docker compose up -d`

---

## 2. Start the Flink Telemetry Processor
This service consumes raw telemetry from the remote Kafka cluster and enriches it.

```powershell
# Navigate to the flink-processor directory
cd flink-processor

# Build and start the processor
# --build ensures your local code changes in job.py are included
docker compose -f docker-compose.dev.yml up --build -d
```

---

## 3. Monitor the Pipeline
Use these commands to verify that data is flowing correctly.

### Check Processor Logs
```powershell
docker compose -f docker-compose.dev.yml logs -f flink-processor
```

### Run the Processed Data Consumer (Local)
This script monitors the output of the Flink processor on the `waste.bin.processed` topic.
```powershell
cd app-consumer
python .\processed_consumer.py
```

### Run the Raw Telemetry Consumer (Local)
This script monitors the incoming raw data from the edge devices.
```powershell
cd app-consumer
python .\kafka_consumer.py
```

---

## 4. Troubleshooting

| Issue | Resolution |
| :--- | :--- |
| **No tables in Postgres** | Run `docker compose down -v` in the `db` folder to clear the old volume, then restart. |
| **Kafka Connection Hang** | Ensure you are using `Manual Assign` mode in `job.py` to bypass Group Coordinator issues on remote clusters. |
| **InfluxDB Buckets Missing** | Check the logs of the `influxdb-setup` container to ensure the bucket creation script finished. |

---

## Infrastructure Summary
- **PostgreSQL**: `localhost:5432` (User: `waste_admin`)
- **InfluxDB UI**: `http://localhost:8086` (Token: `my-super-token`)
- **Kafka Broker**: `163.47.8.3:9094` (Remote)
