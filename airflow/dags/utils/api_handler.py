import os
import logging
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

def reload_ml_service():
    """Trigger ml-service to reload production models."""
    base_url = os.getenv("ML_SERVICE_URL", "http://ml-service:8000")
    url = f"{base_url}/internal/models/reload"
    
    try:
        req = Request(url, method="POST")
        with urlopen(req, timeout=10) as res:
            logger.info(f"ML Service Reloaded: {res.read().decode()}")
    except Exception as e:
        logger.error(f"ML Service Reload failed: {e}")
