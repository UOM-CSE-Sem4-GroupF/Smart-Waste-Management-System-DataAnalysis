FROM flink:1.20.0-java17

# Install Python 3 and pip
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-dev \
    curl \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create python symlink
RUN ln -s /usr/bin/python3 /usr/bin/python

WORKDIR /app

# Download the Flink-Kafka SQL connector JAR to Flink's lib directory
RUN curl -L https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar \
    -o /opt/flink/lib/flink-sql-connector-kafka.jar

# flink-sql-connector-kafka is a shaded fat JAR — it relocates Kafka classes to
# org.apache.flink.kafka.shaded.*. Java's JAAS framework looks up ScramLoginModule
# by its original class name, which only exists in the standalone kafka-clients JAR.
RUN curl -L https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.3.0/kafka-clients-3.3.0.jar \
    -o /opt/flink/lib/kafka-clients.jar

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Set environment variables for Flink
ENV FLINK_HOME=/opt/flink
ENV PATH=$FLINK_HOME/bin:$PATH

# Use root for now to avoid permission issues with logs/state
USER root

ENTRYPOINT ["python"]
CMD ["job.py", "--mode", "pyflink-kafka"]
