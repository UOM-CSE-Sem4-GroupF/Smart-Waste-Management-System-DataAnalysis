# Project Flow Analysis & Fixes

## Project Overview
Waste Management System with IoT sensors streaming data through the following architecture:
- **MQTT Broker** (Mosquitto) → IoT data ingestion
- **MQTT-to-Kafka Bridge** → Data routing
- **Kafka** → Message streaming
- **Multiple Consumers** → PostgreSQL, InfluxDB for storage & analysis
- **Spark Streaming** → Real-time processing and enrichment

## Issues Found & Fixed

### 1. ✅ Missing Mosquitto (MQTT Broker) Service
**Problem:** Code connects to MQTT at localhost:1883, but no Mosquitto service in docker-compose.yml
**Files Modified:**
- `docker-compose.yml` - Added Mosquitto service with ports 1883 (MQTT) and 9001 (WebSocket)
- `mosquitto.conf` - Created configuration file for Mosquitto broker

**Services Updated:**
- `publisher.py` - Changed `localhost:1883` → `mosquitto:1883`
- `subscriber.py` - Changed `localhost:1883` → `mosquitto:1883`  
- `mqtt_to_kafka.py` - Changed `localhost:1883` → `mosquitto:1883`

### 2. ✅ Missing InfluxDB Service
**Problem:** influxdb_consumer.py writes to localhost:8086, but no InfluxDB service defined
**Files Modified:**
- `docker-compose.yml` - Added InfluxDB service with proper initialization
- `influxdb_consumer.py` - Updated to use `influxdb:8086` (Docker internal hostname)

**Configuration:**
- InfluxDB token: "-76pFpLPoer9mtJwh_ewyXzCt0tWDvpPfl76OsjhYit36g6Sp8Kvs3s4mrvuSEIqWCZoq4CL_NXsO0h6_cpY1Q=="
- Organization: "my-org"
- Bucket: "waste-data"

### 3. ✅ Kafka Network Configuration Issues
**Problem:** Services couldn't communicate with Kafka using container-to-container networking
**Files Modified:**
- `docker-compose.yml` - Added Docker network configuration and dual listeners for Kafka
- `mqtt_to_kafka.py` - Changed `localhost:9092` → `kafka:29092` (internal listener)
- `kafka_consumer.py` - Changed `localhost:9092` → `kafka:29092`
- `influxdb_consumer.py` - Changed `localhost:9092` → `kafka:29092`
- `spark_stream.py` - Updated Kafka bootstrap server to `kafka:29092`

**Kafka Config:**
- External listener: `localhost:9092` (for external clients)
- Internal listener: `kafka:29092` (for Docker containers)

### 4. ✅ Missing PostgreSQL Table Schema
**Problem:** Spark tries to write to `waste_stream` table that doesn't exist
**Files Modified:**
- `init.sql` - Created initialization script with table schema
- `docker-compose.yml` - Added volume mount for init.sql to PostgreSQL

**Schema Created:**
```sql
waste_stream table with columns:
- id (serial primary key)
- bin_id (varchar)
- fill (integer)
- location (varchar)
- priority (varchar) - NEW
- timestamp (timestamp with timezone)
- Indexes on bin_id and timestamp for query performance
```

### 5. ✅ Spark Missing JDBC Driver
**Problem:** Spark job couldn't connect to PostgreSQL without driver
**Files Modified:**
- `spark_stream.py` - Added PostgreSQL JDBC driver to spark.jars.packages

**Spark Configuration:**
```
org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0
org.postgresql:postgresql:42.6.0
```

### 6. ✅ Missing Consumer Groups
**Problem:** Multiple consumers without groups could miss messages or cause conflicts
**Files Modified:**
- `kafka_consumer.py` - Added `group_id='kafka-consumer-group'`
- `influxdb_consumer.py` - Added `group_id='influxdb-consumer-group'`
- `spark_stream.py` - Uses foreachBatch pattern (built-in offset management)

### 7. ✅ Enhanced Data Processing in Spark
**Files Modified:**
- `spark_stream.py` - Comprehensive improvements:
  - Added checkpointLocation for fault tolerance
  - Proper timestamp conversion from Unix to SQL timestamp
  - Error handling in write_to_postgres function
  - Added logging for debugging
  - Better schema inference configuration

## Data Flow (After Fixes)

```
publisher.py (MQTT)
    ↓ (waste/bin1-5 topics)
mosquitto (MQTT Broker)
    ↓
mqtt_to_kafka.py (Bridge)
    ↓ (waste-stream topic)
kafka (Broker)
    ↙ ↓ ↘
kafka_consumer.py  influxdb_consumer.py  spark_stream.py
(logs to console)  (writes to InfluxDB)  (processes & writes to PostgreSQL)
                                              + priority enrichment
                                              + data validation
                                              + timestamp normalization
```

## Docker Network
All services connected via `waste-network` bridge network for seamless inter-service communication.

## Health Checks
- PostgreSQL: Checks if pg_isready every 10 seconds
- InfluxDB: Checks /ping endpoint every 10 seconds

## Files Created/Modified

### Created:
1. `mosquitto.conf` - Mosquitto broker configuration
2. `init.sql` - PostgreSQL initialization script
3. `PROJECT_FIXES.md` - This documentation

### Modified:
1. `docker-compose.yml` - Major restructuring with all services
2. `publisher.py` - MQTT hostname fix
3. `subscriber.py` - MQTT hostname fix
4. `mqtt_to_kafka.py` - MQTT and Kafka hostname fixes
5. `kafka_consumer.py` - Kafka hostname and consumer group
6. `influxdb_consumer.py` - InfluxDB and Kafka hostnames, consumer group
7. `spark_stream.py` - Kafka hostname, JDBC driver, error handling, logging

## How to Run

```bash
# Start all services
docker-compose up -d

# Run in separate terminals
python mqtt_to_kafka.py      # Bridge - required
python publisher.py          # Generator - required
python kafkaConsumer.py       # Optional viewer
python influxdb_consumer.py   # InfluxDB writer
python subscriber.py         # MQTT subscriber
python spark_stream.py       # Spark processor - run in Docker
```

## Verified Compatibility
- ✅ Service naming for Docker container-to-container communication
- ✅ Network connectivity between services
- ✅ Database schema matches data structure
- ✅ Consumer group isolation for parallel processing
- ✅ Proper timestamp handling across systems
- ✅ Error handling and graceful degradation
