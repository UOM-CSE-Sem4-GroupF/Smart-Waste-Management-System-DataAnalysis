# PROJECT PORTABILITY GUIDE

## Zero-Code Deployment System

This project is **100% portable** with environment-based configuration. No code changes needed for any deployment scenario.

---

## How It Works

### Architecture
```
┌─────────────────────────────────────────────────────────────┐
│  All Services (Python, Docker, Config)                      │
│  Read from Single Source: .env file                         │
│  ↓ ↓ ↓                                                       │
│  config.py (Python) → reads .env → config values            │
│  docker-compose.yml → reads .env → service configuration    │
│  spark_stream.py → reads env vars → connection strings      │
└─────────────────────────────────────────────────────────────┘
```

### Configuration Flow
```
.env file (one place to change) 
    ↓
config.py (Python module for local scripts)
    ↓
Python Scripts (publisher.py, consumer.py, etc.) use config values
    ↓
docker-compose.yml (exports env vars to containers)
    ↓
Spark, PostgreSQL, InfluxDB services use env vars
```

---

## Three Deployment Modes - Same Code

### Mode 1: LOCAL Development
**What happens:**
- Python scripts run on localhost
- Services (Kafka, MQTT, Postgres, InfluxDB) run in Docker
- Scripts connect to Docker services via localhost:ports (Docker port-mapping)

**Configuration:**
```env
MQTT_HOST=localhost
KAFKA_HOST=localhost
POSTGRES_HOST=localhost
INFLUXDB_HOST=localhost
DEPLOYMENT_MODE=local
```

**Start:**
```bash
cp .env.example .env
docker-compose up -d
python publisher.py
```

---

### Mode 2: FULL DOCKER
**What happens:**
- Everything runs in Docker containers
- Python scripts containerized (optional)
- Containers communicate via internal Docker network
- External clients connect to container ports

**Configuration:**
```env
MQTT_HOST=mosquitto          # Container name (DNS)
KAFKA_HOST=kafka             # Container name (DNS)
POSTGRES_HOST=postgres       # Container name (DNS)
INFLUXDB_HOST=influxdb       # Container name (DNS)
DEPLOYMENT_MODE=docker
```

**Start:**
```bash
cp .env.docker .env
docker-compose up -d
# All services run in Docker network
```

---

### Mode 3: SERVER Deployment
**What happens:**
- Services deployed to production server
- Python scripts run on server (or separate machines)
- Clients connect to server IPs

**Configuration:**
```env
MQTT_HOST=192.168.1.100      # Your server IP
KAFKA_HOST=192.168.1.100
POSTGRES_HOST=192.168.1.100
INFLUXDB_HOST=192.168.1.100
DEPLOYMENT_MODE=server
```

**Start:**
```bash
cp .env.server .env
# Edit .env with actual server IP
scp . user@server:/app/waste
ssh user@server "cd /app/waste && docker-compose up -d"
```

---

## Key Files

### Configuration
- **`.env`** - Active configuration (switches between modes)
- **`.env.example`** - Template with defaults
- **`.env.docker`** - Docker mode preset
- **`.env.server`** - Server mode preset (edit with server IP)
- **`config.py`** - Python module that reads .env

### Scripts
- **`deploy.sh`** - Linux/Mac deployment script
- **`deploy.bat`** - Windows deployment script
- **`requirements.txt`** - Python package dependencies

### Docker
- **`docker-compose.yml`** - Service definitions (uses .env variables)
- **`mosquitto.conf`** - MQTT broker config
- **`init.sql`** - PostgreSQL table initialization

---

## Installation & Usage

### First Time Setup

```bash
# 1. Install Python packages (local mode only)
pip install -r requirements.txt

# 2. Create .env from example
cp .env.example .env

# 3. Start Docker services
docker-compose up -d

# 4. Verify services running
docker-compose ps
python config.py          # Check loaded configuration
```

### Run Application (LOCAL mode)

Terminal 1 - MQTT Bridge:
```bash
python mqtt_to_kafka.py
```

Terminal 2 - Publisher (data source):
```bash
python publisher.py
```

Terminal 3 - Kafka Consumer (optional viewer):
```bash
python kafka_consumer.py
```

Terminal 4 - InfluxDB Consumer (writes to InfluxDB):
```bash
python influxdb_consumer.py
```

Terminal 5 - MQTT Subscriber (optional viewer):
```bash
python subscriber.py
```

Spark (in Docker):
```bash
docker exec -it spark-master bash
# Inside container:
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.6.0 /path/to/spark_stream.py
```

---

## Switching Between Modes

### LOCAL → SERVER

```bash
# Step 1: Edit server configuration
nano .env.server
# Change: MQTT_HOST=your-server-ip or domain

# Step 2: Copy to active config
cp .env.server .env

# Step 3: Restart services
docker-compose down
docker-compose up -d

# Step 4: Verify connection
python config.py
# Check that all hosts point to server
```

### LOCAL → DOCKER (All containerized)

```bash
# Step 1: Use Docker preset
cp .env.docker .env

# Step 2: Update configs
docker-compose down
docker-compose up -d

# Step 3: Verify
docker-compose ps
# All services should show as running
```

---

## For Different Environments

### Development Machine
```env
DEPLOYMENT_MODE=local
MQTT_HOST=localhost
KAFKA_HOST=localhost
POSTGRES_PASSWORD=devpass123
```

### Staging Server
```env
DEPLOYMENT_MODE=server
MQTT_HOST=staging.example.com
KAFKA_HOST=staging.example.com
POSTGRES_PASSWORD=<secret>
```

### Production Server
```env
DEPLOYMENT_MODE=server
MQTT_HOST=prod.example.com
KAFKA_HOST=prod.example.com
POSTGRES_PASSWORD=<secure-secret>
INFLUXDB_TOKEN=<secure-token>
```

---

## Environment Variables Reference

All configuration variables:

```env
# MQTT
MQTT_HOST           - Hostname/IP of MQTT broker
MQTT_PORT           - MQTT port (default: 1883)

# Kafka (external)
KAFKA_HOST          - Hostname/IP of Kafka (for external clients)
KAFKA_PORT          - Kafka port (default: 9092)

# Kafka (internal to Docker)
KAFKA_INTERNAL_HOST - For Docker containers (default: kafka)
KAFKA_INTERNAL_PORT - Internal port (default: 29092)

# PostgreSQL
POSTGRES_HOST       - Database server hostname
POSTGRES_PORT       - Database port (default: 5432)
POSTGRES_DB         - Database name
POSTGRES_USER       - Database user
POSTGRES_PASSWORD   - Database password

# InfluxDB
INFLUXDB_HOST       - InfluxDB server hostname
INFLUXDB_PORT       - InfluxDB port (default: 8086)
INFLUXDB_ORG        - Organization name
INFLUXDB_BUCKET     - Bucket name
INFLUXDB_TOKEN      - Authentication token

# Mode
DEPLOYMENT_MODE     - local, docker, or server
```

---

## Verification Checklist

### Configuration Loaded?
```bash
python config.py
```
Should print all configuration values from .env

### Services Running?
```bash
docker-compose ps
```
Should show all containers with status "Up"

### Python Connectivity?
```bash
python -c "from config import *; print(KAFKA_BOOTSTRAP_SERVERS)"
```
Should print correct bootstrap server

### MQTT Working?
```bash
python subscriber.py
```
Should connect successfully

### Kafka Working?
```bash
python kafka_consumer.py
```
Should connect and wait for messages

---

## Troubleshooting

### "Connection refused" on Python script
→ Check `.env` file - is the hostname correct for your environment?
→ LOCAL: localhost
→ SERVER: your server IP

### Services not accessible
→ Check if Docker services are running: `docker-compose ps`
→ Check firewall rules for ports (1883, 9092, 5432, 8086)

### Config values not loading
→ Ensure `.env` file exists in project root
→ Run: `python -m dotenv list` to debug
→ Check file permissions

### Different hostnames needed between modes
→ That's OK! Use different `.env` files:
  - `.env.dev` for development
  - `.env.prod` for production  
  - Switch: `cp .env.prod .env && docker-compose up -d`

---

## CI/CD Integration

### In GitHub Actions:
```yaml
- name: Deploy
  env:
    MQTT_HOST: ${{ secrets.SERVER_IP }}
    KAFKA_HOST: ${{ secrets.SERVER_IP }}
    POSTGRES_PASSWORD: ${{ secrets.DB_PASSWORD }}
  run: |
    cp .env.example .env
    docker-compose up -d
```

### Docker Build:
```dockerfile
FROM python:3.9
WORKDIR /app
COPY . .
RUN pip install -r requirements.txt

# Uses .env from build context
CMD ["python", "publisher.py"]
```

---

## Summary

✅ **Zero code changes** for different deployments
✅ **Single configuration file** (`.env`)
✅ **Three preset templates** (`.env.*`)
✅ **Portable across machines** and platforms
✅ **Production-ready** environment-based configuration
