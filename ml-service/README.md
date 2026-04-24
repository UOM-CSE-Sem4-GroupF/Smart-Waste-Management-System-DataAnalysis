# ML Service (Member 4)

## Role
Provide prediction APIs.

---

## Input

HTTP Requests

Example:
GET /predict/fill-time?fill=80

---

## Frameworks
- FastAPI
- Python
- Uvicorn

---

## Database Access
NONE (keep simple)

---

## Processing

### Fill Prediction
hours_until_full = (100 - fill) / 5

---

### Route Scoring
Return static score

---

## Output

### API Response

{
  "predicted_full_at": "2026-04-15T15:00:00Z",
  "confidence": "medium"
}

---

## Deliverables
- main.py
- Dockerfile