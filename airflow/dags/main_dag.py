from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
from kafka import KafkaProducer

def publish_kafka_event():
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    message = {
        "model_version": "v1",
        "timestamp": datetime.now().isoformat(),
        "source_service": "airflow"
    }
    producer.send('waste.model.retrained', message)
    producer.flush()
    producer.close()

with DAG(
    dag_id="waste_spark_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    run_spark = BashOperator(
        task_id="run_spark_job",
        bash_command="docker run --rm --network db_default my-spark-job"
    )

    publish_event = PythonOperator(
        task_id="publish_kafka_event",
        python_callable=publish_kafka_event
    )

    run_spark >> publish_event
