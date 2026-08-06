import os
import logging
import mlflow
from mlflow.tracking import MlflowClient

logger = logging.getLogger(__name__)

def get_client():
    uri = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    mlflow.set_tracking_uri(uri)
    return MlflowClient(tracking_uri=uri)

def register_latest_models(models_dict):
    """Register the latest run models."""
    client = get_client()
    for key, name in models_dict.items():
        try:
            # Search for latest run of this model in the experiment
            exp = mlflow.get_experiment_by_name("waste-model-training")
            if not exp: continue
            
            runs = client.search_runs(
                experiment_ids=[exp.experiment_id],
                max_results=1,
                order_by=["start_time DESC"]
            )
            
            if runs:
                run_id = runs[0].info.run_id
                model_uri = f"runs:/{run_id}/model"
                mlflow.register_model(model_uri, name)
                logger.info(f"Registered {name} from run {run_id}")
        except Exception as e:
            logger.error(f"Failed to register {name}: {e}")

def promote_to_production(models_dict):
    """Transition models to Production stage."""
    client = get_client()
    for name in models_dict.values():
        try:
            versions = client.get_latest_versions(name, stages=None)
            if versions:
                latest = max(versions, key=lambda v: int(v.version))
                client.transition_model_version_stage(
                    name=name,
                    version=latest.version,
                    stage="Production",
                    archive_existing_versions=True
                )
                logger.info(f"Promoted {name} v{latest.version} to Production")
        except Exception as e:
            logger.error(f"Promotion failed for {name}: {e}")
