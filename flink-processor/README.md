# Flink Processor (Member 1)

## Role
Real-time stream processor.

Kafka → Flink → PostgreSQL + Kafka

---

## Input

### Kafka Topic
waste.bin.telemetry

### Format
{
  "bin_id": "BIN-001",
  "fill_level_pct": 80,
  "battery_level_pct": 70,
  "timestamp": "..."
}

---

## Frameworks
- PyFlink
- Kafka Connector
- PostgreSQL (psycopg2 or JDBC)

---

## Database Access

READ:
- bins (volume_litres)
- waste_categories (avg_kg_per_litre)

WRITE:
- bin_current_state (UPSERT)

---

## Processing

1. Enrich data from DB
2. Compute:

estimated_weight_kg = fill_pct × volume × avg_kg_per_litre

3. Classify:

<50 → normal  
50–75 → monitor  
75–90 → urgent  
>90 → critical  

4. urgency_score = fill_level_pct

---

## Output

### Kafka
Topic: waste.bin.processed

{
  "bin_id": "BIN-001",
  "fill_level_pct": 80,
  "status": "urgent",
  "urgency_score": 80,
  "estimated_weight_kg": 120,
  "timestamp": "...",
  "source_service": "flink"
}

---

### PostgreSQL
Table: bin_current_state

Fields updated:
- fill_level_pct
- estimated_weight_kg
- status
- urgency_score
- last_reading_at

---

## Deliverables
- job.py
- Dockerfile