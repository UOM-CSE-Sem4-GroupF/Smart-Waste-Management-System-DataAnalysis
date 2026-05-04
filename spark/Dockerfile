FROM eclipse-temurin:11-jdk-focal

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    wget \
    && rm -rf /var/lib/apt/lists/*

RUN pip install pyspark==3.3.4 kafka-python

# Download PostgreSQL JDBC driver
RUN mkdir -p /opt/spark/jars && \
    wget -O /opt/spark/jars/postgresql-42.6.0.jar https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

COPY job.py /opt/spark-apps/job.py

WORKDIR /opt/spark-apps

ENV SPARK_CLASSPATH=/opt/spark/jars/postgresql-42.6.0.jar

CMD ["spark-submit", "--jars", "/opt/spark/jars/postgresql-42.6.0.jar", "job.py"]