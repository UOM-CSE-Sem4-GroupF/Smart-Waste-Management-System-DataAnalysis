FROM apache/airflow:2.5.0

USER airflow

COPY dags/ /opt/airflow/dags/

# Install kafka-python for publishing events
RUN pip install kafka-python