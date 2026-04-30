from datetime import datetime, timedelta, timezone
import logging
import os
from typing import Any

from app.core.config import LOADED_AT_UTC


WASTE_DENSITY_KG_PER_L = {
    "food_waste": 0.9,
    "paper": 0.1,
    "glass": 2.5,
    "plastic": 0.05,
    "general": 0.3,
    "e_waste": 3.2,
}


class PredictionService:
    def __init__(self) -> None:

        self.model_version = "baseline-2026.04"
        self.loaded_at = LOADED_AT_UTC
        self._logger = logging.getLogger("ml-service.predictor")
        self._models: dict[str, Any] = {}
        self.mlflow_enabled = False
        self.mlflow_tracking_uri = ""
        self._mlflow_run_id = None
    def load_models(self) -> None:
        """Load production models from MLflow if configured; fallback gracefully."""

        tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "").strip()
        if not tracking_uri:
            self._logger.info("MLflow tracking URI not set; using baseline heuristics.")
            return
        
        self.mlflow_tracking_uri = tracking_uri
        try:
            import mlflow
        except Exception:
            self._logger.warning("MLflow package not installed; using baseline heuristics.")
            return

        stage = os.getenv("MLFLOW_MODEL_STAGE", "Production")
        model_names = {
            "fill_time": os.getenv("MLFLOW_FILL_MODEL_NAME", "waste-fill-time-model"),
            "zone_generation": os.getenv("MLFLOW_ZONE_MODEL_NAME", "waste-zone-generation-model"),
            "route_score": os.getenv("MLFLOW_ROUTE_MODEL_NAME", "waste-route-score-model"),
        }

        mlflow.set_tracking_uri(tracking_uri)
        loaded_count = 0

        for key, model_name in model_names.items():
            model_uri = f"models:/{model_name}/{stage}"
            try:
                self._models[key] = mlflow.pyfunc.load_model(model_uri)
                loaded_count += 1
            except Exception as exc:
                self._logger.warning("Could not load MLflow model %s: %s", model_uri, exc)

        if loaded_count:
            self.mlflow_enabled = True
            self.model_version = f"mlflow-{stage.lower()}"
            self.loaded_at = datetime.now(timezone.utc)
            self._logger.info("Loaded %d MLflow model(s).", loaded_count)

    def _predict_from_model(self, model_key: str, features: dict[str, float | int]) -> float | None:
        model = self._models.get(model_key)
        if model is None:
            return None

        try:
            import pandas as pd

            result = model.predict(pd.DataFrame([features]))
            return float(result[0])
        except Exception as exc:
            self._logger.warning("Model inference failed for %s: %s", model_key, exc)
            return None

    def predict_fill_time(self, current_fill_level: float) -> tuple[datetime, dict[str, float]]:
        hours_until_full = self._predict_from_model(
            "fill_time", {"current_fill_level_pct": current_fill_level}
        )

        if hours_until_full is None:
            # Baseline heuristic used when model is unavailable.
            fill_rate_pct_per_hour = 6.0 if current_fill_level >= 75 else 4.0
            hours_until_full = max(0.0, (100.0 - current_fill_level) / fill_rate_pct_per_hour)

        now = datetime.now(timezone.utc)
        predicted_full_at = now + timedelta(hours=hours_until_full)
        margin = max(0.5, hours_until_full * 0.2)
        return predicted_full_at, {
            "lower_hours": round(max(0.0, hours_until_full - margin), 2),
            "upper_hours": round(hours_until_full + margin, 2),
        }

    def predict_zone_generation(self, zone_id: int, date_range: str) -> tuple[float, dict[str, float]]:
        period_hint = 7 if "week" in date_range.lower() else 1
        predicted_kg_per_day = self._predict_from_model(
            "zone_generation",
            {
                "zone_id": zone_id,
                "period_hint": period_hint,
            },
        )

        if predicted_kg_per_day is None:
            factor = 1 + (zone_id % 5) * 0.05
            base = 640.0 * factor
            predicted_kg_per_day = round(base, 2)

        breakdown = {
            "food_waste": round(predicted_kg_per_day * 0.35, 2),
            "paper": round(predicted_kg_per_day * 0.16, 2),
            "glass": round(predicted_kg_per_day * 0.11, 2),
            "plastic": round(predicted_kg_per_day * 0.18, 2),
            "general": round(predicted_kg_per_day * 0.15, 2),
            "e_waste": round(predicted_kg_per_day * 0.05, 2),
        }
        return round(predicted_kg_per_day, 2), breakdown

    def score_route(self, vehicle_max_cargo_kg: float, stop_weights: list[float]) -> tuple[float, list[str]]:
        total_weight = sum(stop_weights)
        utilisation = total_weight / vehicle_max_cargo_kg if vehicle_max_cargo_kg else 0
        score = self._predict_from_model(
            "route_score",
            {
                "vehicle_max_cargo_kg": vehicle_max_cargo_kg,
                "total_weight_kg": total_weight,
                "utilisation_ratio": utilisation,
                "stop_count": len(stop_weights),
            },
        )

        if score is None:
            score = max(0.0, min(100.0, 100 - abs(0.78 - utilisation) * 120))

        suggestions: list[str] = []
        if utilisation > 0.95:
            suggestions.append("Split route: projected weight is close to vehicle limit.")
        elif utilisation < 0.55:
            suggestions.append("Route under-utilized: consider merging nearby bin stops.")
        else:
            suggestions.append("Cargo utilization is balanced for current vehicle capacity.")

        if len(stop_weights) > 40:
            suggestions.append("High stop count: evaluate clustering to reduce travel overhead.")

        return round(score, 2), suggestions

    def waste_generation_trends(self, zone_id: int, period: str) -> list[dict[str, float | str]]:
        length_by_period = {"week": 7, "month": 4, "quarter": 3}
        points = length_by_period[period]
        base = self._predict_from_model(
            "zone_generation",
            {
                "zone_id": zone_id,
                "period_hint": points,
            },
        )

        if base is None:
            base = 520 + (zone_id % 7) * 18

        data: list[dict[str, float | str]] = []

        for idx in range(points):
            multiplier = 1 + (idx * 0.03)
            total = base * multiplier
            data.append(
                {
                    "label": f"P{idx + 1}",
                    "food_waste": round(total * 0.34, 2),
                    "paper": round(total * 0.17, 2),
                    "glass": round(total * 0.1, 2),
                    "plastic": round(total * 0.2, 2),
                    "general": round(total * 0.14, 2),
                    "e_waste": round(total * 0.05, 2),
                }
            )

        return data

    def log_prediction_metric(self, metric_name: str, value: float, step: int = 0) -> None:
        """Log prediction metrics to MLflow if enabled."""
        if not self.mlflow_enabled or not self.mlflow_tracking_uri:
            return
        
        try:
            import mlflow
            mlflow.set_tracking_uri(self.mlflow_tracking_uri)
            
            # Use a shared experiment or create one for predictions
            experiment_name = "waste-ml-service-predictions"
            try:
                exp_id = mlflow.create_experiment(experiment_name)
            except Exception:
                exp = mlflow.get_experiment_by_name(experiment_name)
                exp_id = exp.experiment_id if exp else None
            
            if exp_id:
                if not mlflow.active_run():
                    mlflow.start_run(experiment_id=exp_id)
                mlflow.log_metric(metric_name, value, step=step)
        except Exception as exc:
            self._logger.debug("Could not log metric to MLflow: %s", exc)


predictor = PredictionService()
