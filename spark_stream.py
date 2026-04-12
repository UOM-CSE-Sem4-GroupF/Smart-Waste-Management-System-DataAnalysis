import os
import subprocess
import sys
import time
import uuid

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    # Container side may not have python-dotenv; environment variables still work.
    pass

DEPLOYMENT_MODE = os.getenv("DEPLOYMENT_MODE", "local")
KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")
KAFKA_PORT = int(os.getenv("KAFKA_PORT", "9092"))
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "waste_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "yourpassword")

def run_in_docker_from_windows() -> int:
    script_host = os.path.abspath(__file__)
    script_container = "/tmp/spark_stream.py"

    copy_cmd = ["docker", "cp", script_host, f"spark-master:{script_container}"]
    run_cmd = [
        "docker",
        "exec",
        "-e",
        "SPARK_DELEGATED=1",
        "-e",
        f"DEPLOYMENT_MODE={DEPLOYMENT_MODE}",
        "-e",
        f"KAFKA_HOST={KAFKA_HOST}",
        "-e",
        f"KAFKA_PORT={KAFKA_PORT}",
        "-e",
        f"KAFKA_INTERNAL_HOST={os.getenv('KAFKA_INTERNAL_HOST', 'kafka')}",
        "-e",
        f"KAFKA_INTERNAL_PORT={os.getenv('KAFKA_INTERNAL_PORT', '29092')}",
        "-e",
        f"POSTGRES_HOST={POSTGRES_HOST}",
        "-e",
        f"POSTGRES_PORT={POSTGRES_PORT}",
        "-e",
        f"POSTGRES_DB={POSTGRES_DB}",
        "-e",
        f"POSTGRES_USER={POSTGRES_USER}",
        "-e",
        f"POSTGRES_PASSWORD={POSTGRES_PASSWORD}",
        "spark-master",
        "/spark/bin/spark-submit",
        "--packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.6.0",
        script_container,
    ]

    print("Windows detected. Delegating Spark streaming job to spark-master container...")
    subprocess.run(copy_cmd, check=True)
    return subprocess.run(run_cmd).returncode


def main():
    # Spark Structured Streaming on Windows commonly requires Hadoop native binaries.
    # Delegate to the Linux Spark container to keep local run command unchanged.
    if os.name == "nt" and os.getenv("SPARK_DELEGATED") != "1":
        exit_code = run_in_docker_from_windows()
        if exit_code != 0:
            raise SystemExit(exit_code)
        return

    # Ensure PySpark uses the active interpreter.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, from_json, from_unixtime, when
    from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

    # Use internal Docker hosts only when running inside a container.
    is_container = os.path.exists("/.dockerenv")

    default_checkpoint_root = "/tmp/spark-checkpoint" if is_container else "./spark-checkpoint"
    checkpoint_root = os.getenv("SPARK_CHECKPOINT_DIR", default_checkpoint_root)
    run_suffix = f"{int(time.time())}_{uuid.uuid4().hex[:8]}"

    # If caller did not pin a checkpoint path, use a per-run path to avoid
    # concurrent checkpoint log update collisions from multiple active jobs.
    if "SPARK_CHECKPOINT_DIR" not in os.environ:
        checkpoint_root = checkpoint_root.rstrip("/\\") + "_" + run_suffix

    checkpoint_dir = os.path.abspath(checkpoint_root)
    os.makedirs(checkpoint_dir, exist_ok=True)
    checkpoint_uri = "file:///" + checkpoint_dir.replace("\\", "/")

    if is_container and KAFKA_HOST in ("localhost", "127.0.0.1"):
        kafka_host = os.getenv("KAFKA_INTERNAL_HOST", "kafka")
        kafka_port = os.getenv("KAFKA_INTERNAL_PORT", "29092")
    else:
        kafka_host = KAFKA_HOST
        kafka_port = str(KAFKA_PORT)

    if is_container and POSTGRES_HOST in ("localhost", "127.0.0.1"):
        postgres_host = "postgres"
    else:
        postgres_host = POSTGRES_HOST

    kafka_bootstrap = f"{kafka_host}:{kafka_port}"

    spark = SparkSession.builder \
        .appName("WasteStreaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.6.0") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    print(f"Deployment mode: {DEPLOYMENT_MODE}")
    print(f"Connecting to Kafka at {kafka_bootstrap}")

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("subscribe", "waste-stream") \
        .option("startingOffsets", "earliest") \
        .load()

    schema = StructType(
        [
            StructField("bin_id", StringType()),
            StructField("fill", IntegerType()),
            StructField("location", StringType()),
            StructField("timestamp", DoubleType()),
        ]
    )

    json_df = df.selectExpr("CAST(value AS STRING)")
    json_df = json_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

    clean_df = json_df.filter((col("fill") >= 0) & (col("fill") <= 100))

    alert_df = clean_df.withColumn(
        "priority",
        when(col("fill") > 80, "HIGH").when(col("fill") > 50, "MEDIUM").otherwise("LOW"),
    )

    alert_df = alert_df.withColumn("timestamp", from_unixtime(col("timestamp")).cast("timestamp"))

    def write_to_postgres(batch_df, batch_id):
        try:
            postgres_url = f"jdbc:postgresql://{postgres_host}:{POSTGRES_PORT}/{POSTGRES_DB}"
            batch_df.write \
                .format("jdbc") \
                .option("url", postgres_url) \
                .option("dbtable", "waste_stream") \
                .option("user", POSTGRES_USER) \
                .option("password", POSTGRES_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            print(f"Batch {batch_id} successfully written to PostgreSQL")
        except Exception as e:
            print(f"Error writing batch {batch_id}: {str(e)}")

    query = alert_df.writeStream \
        .foreachBatch(write_to_postgres) \
        .outputMode("append") \
        .queryName(f"waste_stream_{run_suffix}") \
        .option("checkpointLocation", checkpoint_uri) \
        .start()

    print("Spark Streaming job started...")
    query.awaitTermination()


if __name__ == "__main__":
    main()