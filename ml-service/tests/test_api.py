from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_health() -> None:
    response = client.get("/health")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["service"] == "fastapi-ml-service"


def test_predict_fill_time() -> None:
    response = client.get(
        "/api/v1/ml/predict/fill-time",
        params={"bin_id": "BIN-047", "current_fill_level": 84.5},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["bin_id"] == "BIN-047"
    assert "predicted_full_at" in payload
    assert "confidence_interval" in payload


def test_predict_fill_time_invalid_fill_level() -> None:
    response = client.get(
        "/api/v1/ml/predict/fill-time",
        params={"bin_id": "BIN-047", "current_fill_level": 120},
    )
    assert response.status_code == 422


def test_predict_zone_generation() -> None:
    response = client.get(
        "/api/v1/ml/predict/zone-generation",
        params={"zone_id": 3, "date_range": "next_week"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["zone_id"] == 3
    assert "by_waste_category" in payload


def test_score_route() -> None:
    response = client.post(
        "/api/v1/ml/score/route",
        json={
            "zone_id": 2,
            "route_type": "routine",
            "vehicle_max_cargo_kg": 3000,
            "stops": [
                {"bin_id": "BIN-001", "estimated_weight_kg": 220},
                {"bin_id": "BIN-002", "estimated_weight_kg": 180},
            ],
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert 0 <= payload["efficiency_score"] <= 100


def test_waste_generation_trends() -> None:
    response = client.get(
        "/api/v1/ml/trends/waste-generation",
        params={"zone_id": 1, "period": "week"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["period"] == "week"
    assert len(payload["series"]) == 7
