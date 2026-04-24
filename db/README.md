# Database Setup (Member 2)

## Role

Provide database for all services

---

## Frameworks

- PostgreSQL
- Docker

---

## Quick Start (Exact Details)

Run all commands from this folder:

`Smart-Waste-Management-System-DataAnalysis/db`

### Start Database

```powershell
docker compose up -d
docker compose ps
```

Expected service/container:

- Service name: `postgres`
- Container name: `db-postgres-1`
- Image: `postgres:16-alpine`

### Database Configuration

These are defined in `docker-compose.yml`:

- Host: `localhost`
- Port: `5432`
- Database: `waste_management`
- Username: `waste_admin`
- Password: `waste_admin_password`

Connection string:

```text
postgresql://waste_admin:waste_admin_password@localhost:5432/waste_management
```

### Connect and Verify

Open psql in the running container:

```powershell
docker exec -it db-postgres-1 psql -U waste_admin -d waste_management
```

Inside psql, verify schema:

```sql
SELECT current_database(), current_user;
SELECT count(*) FROM information_schema.tables WHERE table_schema='public';
\dt
```

Expected table count from `init.sql`: `15`

### Important Behavior

- `init.sql` runs only on first initialization of the Postgres volume.
- If schema changes are made in `init.sql`, reinitialize:

```powershell
docker compose down -v
docker compose up -d
```

### Stop Database

```powershell
docker compose down
```

---

## Time-Series Layer (Kafka -> Flink -> InfluxDB)

This folder now includes InfluxDB for real-time analytics storage.

Data flow:

`Kafka (waste.bin.telemetry) -> Flink (processing/enrichment) -> InfluxDB`

### Running InfluxDB

#### 1. Setup Environment Variables

Before first run, copy the example file and configure:

```powershell
cp .env.example .env
# Edit .env with your credentials (optional, defaults will work for local dev)
```

#### 2. Start All Services (PostgreSQL + InfluxDB)

```powershell
docker compose up -d
docker compose ps
```

Expected services:

- `postgres` (db-postgres-1) - should show "Up (healthy)"
- `influxdb` (db-influxdb-1) - should show "Up (healthy)"
- `influxdb-setup` (db-influxdb-setup-1) - runs once then exits (this is normal)

#### 3. Verify InfluxDB is Ready

```powershell
docker compose logs influxdb-setup --tail 30
```

You should see output like:

```
Created bucket bin_readings_raw (retention: 8760h).
Created bucket bin_readings_processed (retention: 2160h).
Created bucket vehicle_positions (retention: 8760h).
Created bucket zone_statistics (retention: 17520h).
Created bucket waste_generation_trends (retention: forever).
InfluxDB bucket setup completed.
```

#### 4. Access InfluxDB UI

Open browser and navigate to:

- **URL**: `http://localhost:8086`
- **Username**: `admin` (from `.env` or defaults)
- **Password**: `admin12345` (from `.env` or defaults)
- **Organization**: `waste-org` (from `.env` or defaults)

#### 5. Verify Buckets in UI

1. Click on "Buckets" in left sidebar
2. You should see all 5 buckets:
   - `bin_readings_raw`
   - `bin_readings_processed`
   - `vehicle_positions`
   - `zone_statistics`
   - `waste_generation_trends`

#### 6. Test Write/Read (PowerShell Example)

Write a test point:

```powershell
curl.exe -X POST "http://localhost:8086/api/v2/write?org=waste-org&bucket=bin_readings_processed&precision=s" `
  -H "Authorization: Token my-super-token" `
  --data-raw "bin_readings_processed,bin_id=TEST-001,zone_id=Z1,waste_category=test,status=ok fill_level_pct=75.5,urgency_score=50i,estimated_weight_kg=30.0"
```

Query it back:

```powershell
$body = @'
from(bucket: "bin_readings_processed")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "bin_readings_processed")
  |> limit(n: 5)
'@

Invoke-RestMethod -Method Post -Uri "http://localhost:8086/api/v2/query?org=waste-org" `
  -Headers @{Authorization="Token my-super-token";"Content-type"="application/vnd.flux"} `
  -Body $body
```

#### 7. Stop Services

```powershell
docker compose down
```

To also remove volumes (warning: this deletes data):

```powershell
docker compose down -v
```

### Start Services

Both PostgreSQL and InfluxDB start together with:

```powershell
docker compose up -d
docker compose ps
```

InfluxDB UI:

- URL: `http://localhost:8086`
- Username: `admin` (from `.env`/`.env.example`)
- Password: `admin12345` (from `.env`/`.env.example`)

### Influx Buckets and Retention

The `influxdb-setup` service creates these buckets automatically:

- `bin_readings_raw` -> 1 year
- `bin_readings_processed` -> 90 days
- `vehicle_positions` -> 1 year
- `zone_statistics` -> 2 years
- `waste_generation_trends` -> forever

### Measurement Contract for Flink Writers

#### 1) bin_readings_raw

- Tags: `bin_id`, `zone_id`, `waste_category`
- Fields: `fill_level_pct`, `battery_level_pct`, `signal_strength`, `temperature_c`

#### 2) bin_readings_processed

- Tags: `bin_id`, `zone_id`, `waste_category`, `status`
- Fields: `fill_level_pct`, `urgency_score`, `estimated_weight_kg`, `fill_rate_pct_per_hour`, `predicted_full_hours`

#### 3) vehicle_positions

- Tags: `vehicle_id`, `driver_id`, `job_id`, `zone_id`
- Fields: `lat`, `lng`, `speed_kmh`, `heading_degrees`, `cargo_weight_kg`

#### 4) zone_statistics

- Tags: `zone_id`, `waste_category`
- Fields: `avg_fill_level`, `urgent_count`, `total_bins`, `total_weight_kg`

#### 5) waste_generation_trends

- Tags: `zone_id`, `waste_category`, `day_of_week`
- Fields: `avg_daily_kg`, `avg_fill_rate`, `peak_hour`

### Tag/Field Rule

- Use tags only for low-cardinality filters.
- Keep frequently changing numeric values as fields.
- Do not store high-cardinality values as tags.

### Manual Write Test

```powershell
curl -X POST "http://localhost:8086/api/v2/write?org=waste-org&bucket=bin_readings_processed&precision=s" ^
	-H "Authorization: Token my-super-token" ^
	--data-raw "bin_readings_processed,bin_id=BIN-1,zone_id=ZONE-1,waste_category=food_waste,status=urgent fill_level_pct=75.2,urgency_score=78i,estimated_weight_kg=41.7,fill_rate_pct_per_hour=2.8,predicted_full_hours=8.0"
```

### Manual Query Test (Flux)

```flux
from(bucket: "bin_readings_processed")
	|> range(start: -1h)
	|> filter(fn: (r) => r._measurement == "bin_readings_processed")
```

### Environment Template

Use `db/.env.example` as template for local values.

---

## Responsibilities

1. Create schema
2. Insert sample data
3. Provide DB connection

---

## Tables

- waste_categories
- bins
- vehicles
- bin_current_state
- route_plans

---

## Input

None

---

## Output

### SQL Script

init.sql

---

### Running Database

Accessible via docker-compose

---

## Deliverables

- docker-compose.yml
- init.sql
