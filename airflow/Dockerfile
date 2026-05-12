FROM apache/airflow:2.5.0

# Create airflow home with proper permissions
USER root
RUN mkdir -p /airflow/{dags,logs,plugins} && \
    chmod -R 777 /airflow

USER airflow

COPY dags/ /opt/airflow/dags/

# Install dependencies for custom tasks
RUN pip install kafka-python mlflow