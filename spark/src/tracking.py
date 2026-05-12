import logging
import mlflow
import mlflow.sklearn
from .config import MLFLOW_TRACKING_URI, EXPERIMENT_NAME

logger = logging.getLogger(__name__)

def init_mlflow():
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    try:
        mlflow.create_experiment(EXPERIMENT_NAME)
    except:
        pass
    mlflow.set_experiment(EXPERIMENT_NAME)

def log_model(model, name, metrics, params=None):
    """Log model to MLflow with metrics."""
    with mlflow.start_run(run_name=name, nested=True):
        mlflow.log_metrics(metrics)
        if params:
            mlflow.log_params(params)
        mlflow.sklearn.log_model(model, "model", registered_model_name=name)
        logger.info(f"Logged model {name} to MLflow")
