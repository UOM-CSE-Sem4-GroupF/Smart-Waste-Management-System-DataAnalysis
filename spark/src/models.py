import logging
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.model_selection import GridSearchCV, train_test_split
from sklearn.metrics import mean_absolute_error, r2_score
import numpy as np

logger = logging.getLogger(__name__)

def train_regression_model(pdf, features, label, model_type="rf"):
    """Generic trainer with GridSearchCV."""
    if pdf.empty:
        return None, {}

    # Check if we have enough data to split
    if len(pdf) < 5:
        print(f"Skipping training: Not enough data (found {len(pdf)} rows, need at least 5)")
        return None, {}

    X = pdf[features]
    y = pdf[label]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    if model_type == "rf":
        model = RandomForestRegressor(random_state=42)
        params = {"n_estimators": [50, 100], "max_depth": [5, 10]}
    else:
        model = GradientBoostingRegressor(random_state=42)
        params = {"n_estimators": [50, 100], "learning_rate": [0.05, 0.1]}

    grid = GridSearchCV(model, params, cv=3, scoring="neg_mean_absolute_error")
    grid.fit(X_train, y_train)
    
    best_model = grid.best_estimator_
    preds = best_model.predict(X_test)
    
    metrics = {
        "mae": float(mean_absolute_error(y_test, preds)),
        "r2": float(r2_score(y_test, preds))
    }
    
    # Extract best hyperparams
    best_params = grid.best_params_
    
    return best_model, metrics, best_params

def train_fill_time(pdf):
    features = ["fill_level_pct", "fill_rate_pct_per_hour", "volume_litres", "avg_kg_per_litre", "urgency_score", "hour", "dow"]
    return train_regression_model(pdf, features, "label", "rf")

def train_zone_gen(pdf):
    features = ["avg_fill_level", "urgent_bin_count", "hour", "dow", "lag_1"]
    return train_regression_model(pdf, features, "label", "gb")

def train_route_score(pdf):
    features = ["stops", "weight", "max_cargo", "util"]
    return train_regression_model(pdf, features, "label", "rf")
