from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

from utils.mlflow_handler import register_latest_models, promote_to_production
from utils.kafka_handler import publish_event
from utils.api_handler import reload_ml_service

# Configuration
MODELS = {
    "fill": "waste-fill-time-model",
    "zone": "waste-zone-generation-model",
    "route": "waste-route-score-model"
}

default_args = {
    'owner': 'member_5',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'smart_waste_training_pipeline',
    default_args=default_args,
    description='End-to-end ML Training Pipeline',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['spark', 'mlflow', 'waste'],
) as dag:

    # 1. Run Spark Training Job
    run_spark = BashOperator(
        task_id='run_spark_training',
        bash_command=(
            'docker run --rm --network smart-waste-management-system-dataanalysis_waste-network '
            '-e POSTGRES_HOST=waste-postgres-waste '
            '-e MLFLOW_TRACKING_URI=http://waste-mlflow:5000 '
            'my-spark-job:latest'
        ),
    )

    # 2. Register Models in MLflow
    register_models = PythonOperator(
        task_id='register_models',
        python_callable=register_latest_models,
        op_kwargs={'models_dict': MODELS}
    )

    # 3. Promote to Production
    promote_models = PythonOperator(
        task_id='promote_models',
        python_callable=promote_to_production,
        op_kwargs={'models_dict': MODELS}
    )

    # 4. Notify ML Service
    notify_reload = PythonOperator(
        task_id='notify_ml_service',
        python_callable=reload_ml_service
    )

    # 5. Publish Kafka Event
    kafka_notify = PythonOperator(
        task_id='publish_kafka_event',
        python_callable=publish_event,
        op_kwargs={
            'topic': 'waste.model.retrained',
            'event_name': 'model.retrained',
            'models': list(MODELS.values())
        }
    )

    # Pipeline Flow
    run_spark >> register_models >> promote_models >> notify_reload >> kafka_notify
