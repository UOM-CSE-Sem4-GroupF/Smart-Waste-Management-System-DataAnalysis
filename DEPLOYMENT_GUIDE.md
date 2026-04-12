# Deployment Guide - Waste Management System

## Quick Overview

This project is configured to run in **3 deployment modes** without any code changes:

### 1. **LOCAL Mode** (Development - Current)
- Python scripts run on your Windows machine
- Docker services (Broker, Kafka, Databases) run in Docker
- **Use when:** Developing locally or testing code

```bash
# Copy local configuration
cp .env .env.local
# OR on Windows:
copy .env deploy.bat
deploy.bat local start
```

### 2. **DOCKER Mode** (Containerized)
- All services run in Docker containers
- Ideal for CI/CD pipelines and isolated testing
- **Use when:** Testing full containerization or deploying to container orchestration

```bash
# Copy Docker configuration
cp .env.docker .env
docker-compose up -d

# OR using script:
deploy.sh docker start  # Linux/Mac
deploy.bat docker start # Windows
```

### 3. **SERVER Mode** (Production)
- All services run on a dedicated server
- Python scripts and Docker services on the same server
- **Use when:** Deploying to production or staging server

```bash
# Step 1: Edit .env.server with your server's IP/hostname
nano .env.server

# Step 2: Copy to .env
cp .env.server .env

# Step 3: Deploy
docker-compose up -d
```

---

## Environment Variables

Edit the `.env` file to customize deployment:

```env
# MQTT Configuration
MQTT_HOST=localhost          # Change to server IP for server mode
MQTT_PORT=1883

# Kafka Configuration  
KAFKA_HOST=localhost         # Change to server IP for server mode
KAFKA_PORT=9092

# PostgreSQL Configuration
POSTGRES_HOST=localhost      # Change to server IP
POSTGRES_PORT=5432
POSTGRES_DB=waste_db
POSTGRES_USER=postgres
POSTGRES_PASSWORD=yourpassword

# InfluxDB Configuration
INFLUXDB_HOST=localhost      # Change to server IP
INFLUXDB_PORT=8086
INFLUXDB_ORG=my-org
INFLUXDB_BUCKET=waste-data
INFLUXDB_TOKEN=your-token-here

# Deployment Mode
DEPLOYMENT_MODE=local        # local, docker, or server
```

---

## Switching Deployment Environments

### From LOCAL → DOCKER
```bash
cp .env.docker .env
docker-compose down  # Stop local setup
docker-compose up -d # Start Docker setup
```

### From LOCAL → SERVER
```bash
# 1. Edit .env.server with server IP
nano .env.server
# Example: MQTT_HOST=192.168.1.100

# 2. Upload to server
scp .env.server user@server:/opt/waste-management/

# 3. On server:
cp .env.server .env
docker-compose up -d
```

---

## Python Script Configuration

All Python scripts automatically read from `.env` file via `config.py`:

```python
from config import MQTT_HOST, KAFKA_BOOTSTRAP_SERVERS, etc.

client = mqtt.Client()
client.connect(MQTT_HOST, MQTT_PORT, 60)  # Uses config values
```

**No code changes needed** - Just change `.env` file!

---

## Verification

### Check configuration is loaded:
```bash
python config.py
```

### Check if services are running:
```bash
docker-compose ps

# Output:
# NAME         STATUS
# mosquitto    Up
# kafka        Up
# postgres     Up
# influxdb     Up
```

### Test connections:
```bash
# MQTT
python -c "from config import *; print(f'MQTT: {MQTT_HOST}:{MQTT_PORT}')"

# Kafka
python kafka_consumer.py

# PostgreSQL
psql -h $POSTGRES_HOST -U postgres -d waste_db

# InfluxDB
curl http://localhost:8086/ping
```

---

## Deployment Checklist

### For LOCAL Development:
- [ ] `.env` contains `localhost` for services
- [ ] Docker containers are running (`docker-compose ps`)
- [ ] Python packages installed (`pip install -r requirements.txt`)
- [ ] Run `python publisher.py` to start data flow

### For SERVER Deployment:
- [ ] Update `.env.server` with server IP/hostname
- [ ] Copy `.env.server` to server
- [ ] Run `docker-compose up -d` on server
- [ ] Verify all services are running
- [ ] Test connectivity from clients to server

---

## Troubleshooting

### "Connection refused" errors
```
Check .env file - is MQTT_HOST/KAFKA_HOST correct for your environment?
LOCAL mode → should be localhost
SERVER mode → should be server IP address
```

### Services not starting
```bash
docker-compose logs              # View all logs
docker-compose logs kafka        # View specific service logs
```

### Data not flowing
```bash
# Check config
python config.py

# Test MQTT
python subscriber.py

# Test Kafka
python kafka_consumer.py

# Check if services are running
docker-compose ps
```

---

## Scaling & Advanced

### Run on Kubernetes
Adapt the docker-compose.yml to Kubernetes manifests - same config values apply

### CI/CD Integration
Use `.env.server` or pass ENV vars via pipeline:
```yaml
- run: docker-compose up -d
  env:
    MQTT_HOST: ${{ secrets.MQTT_HOST }}
    KAFKA_HOST: ${{ secrets.KAFKA_HOST }}
```

### Multi-environment Management
Create multiple .env files:
- `.env.dev` - Development
- `.env.staging` - Staging  
- `.env.prod` - Production

Switch with: `cp .env.prod .env && docker-compose up -d`

---

## Support

For issues, check:
1. `.env` file variables
2. Docker services status: `docker-compose ps`
3. Service logs: `docker-compose logs [service]`
4. Network connectivity: `ping [MQTT_HOST]`, etc.
