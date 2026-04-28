# Route Optimizer (Member 3)

## Role
Generate routes for urgent bins.

Kafka → Optimizer → PostgreSQL + Kafka

---

## Input

### Kafka
waste.bin.processed

---

### PostgreSQL
- bin_current_state
- bins (location)
- vehicles

---

## Frameworks
- Python
- Kafka consumer
- PostgreSQL (psycopg2)
- Optional: OR-Tools

---

## Processing

1. Filter bins:
urgency_score >= 70

2. Load bin data from DB

3. Sort:
bins by urgency_score DESC

4. Assign to vehicle

5. Compute total weight

---

## Output

### PostgreSQL
Table: route_plans

Fields:
- vehicle_id
- waypoints (JSON array of bin IDs)
- estimated_weight_kg
- route_type = "emergency"

---

### Kafka
Topic: waste.routes.optimized

{
  "job_id": "uuid",
  "vehicle_id": "LORRY-01",
  "bins": ["BIN-1", "BIN-2"],
  "estimated_weight_kg": 250,
  "timestamp": "...",
  "source_service": "optimizer"
}

---

## Deliverables
- app.py
- Dockerfile