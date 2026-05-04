# Airflow (Member 5)

## Role
Batch orchestration

---

## Frameworks
- Apache Airflow
- Python

---

## Input

Optional:
- PostgreSQL (aggregated data)
- Kafka triggers

---

## Processing

DAG:

Task 1:
Run Spark job

Task 2:
Publish Kafka event

---

## Output

Kafka Topic:
waste.model.retrained

{
  "model_version": "v1",
  "timestamp": "...",
  "source_service": "airflow"
}

---

## Deliverables
- dags/main_dag.py
- Dockerfile


---------------------------------------------------------------------------------

# Spark Job (Member 5)

## Role
Batch analytics

---

## Frameworks
- PySpark

---

## Input

PostgreSQL:
- bin_current_state

---

## Processing

Compute:

SELECT zone_id, AVG(estimated_weight_kg)
FROM bin_current_state
GROUP BY zone_id

---

## Output

Option 1:
Print results

Option 2:
Write back to PostgreSQL

---

## Deliverables
- job.py
- Dockerfile