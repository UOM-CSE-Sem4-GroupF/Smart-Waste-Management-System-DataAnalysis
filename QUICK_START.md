# QUICK START - Deploy Without Code Changes

## 1-Minute Setup (LOCAL)

```bash
# Start Docker services
docker-compose up -d

# Terminal 1 - Bridge
python mqtt_to_kafka.py

# Terminal 2 - Producer
python publisher.py

# Terminal 3 - Consumer
python kafka_consumer.py
```

Done! Data flows through the system.

---

## Deploy to SERVER (No Code Changes!)

### Prerequisites
- Server with Docker and Python installed
- Server IP: `192.168.1.100` (example)

### Steps

```bash
# Step 1: Edit configuration for your server
nano .env.server
# Change these lines:
MQTT_HOST=192.168.1.100
KAFKA_HOST=192.168.1.100
POSTGRES_HOST=192.168.1.100
INFLUXDB_HOST=192.168.1.100

# Step 2: Copy to active config
cp .env.server .env

# Step 3: Restart services (automatically use new config)
docker-compose down
docker-compose up -d

# Step 4: Verify
docker-compose ps
```

**That's it!** No Python code was changed. Services reconfigured automatically.

---

## How It Works

**Before (Hardcoded):**
```python
client.connect("localhost", 1883)  # ❌ Hardcoded in code
```

**Now (Environment-based):**
```python
from config import MQTT_HOST, MQTT_PORT
client.connect(MQTT_HOST, MQTT_PORT)  # ✅ Read from .env
```

Change `.env` → Entire system reconfigures → No code edits needed

---

## Configuration Presets

### Development (LOCAL)
```bash
cp .env.example .env
# All services at localhost
```

### Docker (Containerized)
```bash
cp .env.docker .env
# All services use Docker container names
```

### Production (SERVER)
```bash
cp .env.server .env
# Edit with your server IP/domain
```

---

## Key Files

| File | Purpose |
|------|---------|
| `.env` | Current configuration (switch between modes) |
| `.env.example` | Template with defaults |
| `.env.docker` | Docker mode preset |
| `.env.server` | Production mode (edit with server IP) |
| `config.py` | Python module that reads .env |
| `docker-compose.yml` | Uses .env variables for all services |

---

## Verify Configuration

```bash
# See what config is loaded
python config.py

# Example output:
# Deployment Mode: local
# MQTT: localhost:1883
# Kafka: localhost:9092
# PostgreSQL: localhost:5432
# InfluxDB: localhost:8086
```

---

## Examples

### From LOCAL to SERVER

```bash
# 1. Edit server config
MQTT_HOST=production.example.com

# 2. Switch configs
cp .env.server .env

# 3. Services auto-reconfigure
docker-compose down && docker-compose up -d

# ✅ No code changed - everything connected to new server!
```

### Different Environments

```bash
# Development
cp .env.dev .env
docker-compose up -d

# Staging
cp .env.staging .env
docker-compose down && docker-compose up -d

# Production
cp .env.prod .env
docker-compose down && docker-compose up -d
```

---

## What Changed in Project

✅ All hardcoded `localhost` → `${VARIABLE_NAME}`
✅ Created `config.py` - reads .env for Python scripts
✅ Created `.env.*` presets for each deployment mode
✅ Updated `docker-compose.yml` - uses environment variables
✅ All Python scripts import from `config.py`

**Result:** Single .env file controls entire system across any environment.

---

## Environment Variables

```env
MQTT_HOST          # MQTT broker hostname
MYSQL_PORT         # MQTT port
KAFKA_HOST         # Kafka hostname
KAFKA_PORT         # Kafka port
POSTGRES_HOST      # PostgreSQL hostname
POSTGRES_PORT      # PostgreSQL port
POSTGRES_DB        # Database name
POSTGRES_USER      # DB username
POSTGRES_PASSWORD  # DB password
INFLUXDB_HOST      # InfluxDB hostname
INFLUXDB_PORT      # InfluxDB port
INFLUXDB_ORG       # Organization
INFLUXDB_BUCKET    # Bucket name
INFLUXDB_TOKEN     # Auth token
DEPLOYMENT_MODE    # local/docker/server
```

---

## Deployment Commands

```bash
# Show current configuration
python config.py

# Start services with current .env
docker-compose up -d

# Stop services
docker-compose down

# View all services
docker-compose ps

# Check service logs
docker-compose logs -f [service_name]

# Recreate services (applies new .env values)
docker-compose down && docker-compose up -d
```

---

Done! Your project now works **anywhere** without code changes. 🚀
