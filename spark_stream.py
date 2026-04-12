import os
import subprocess
import sys
import time
import uuid
from collections import defaultdict
from datetime import datetime, timedelta

import warnings

warnings.filterwarnings("ignore")

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
INFLUXDB_HOST = os.getenv("INFLUXDB_HOST", "localhost")
INFLUXDB_PORT = int(os.getenv("INFLUXDB_PORT", "8086"))
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "my-org")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "waste-data")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "")
INFLUXDB_URL = f"http://{INFLUXDB_HOST}:{INFLUXDB_PORT}"

# Edit prediction frequencies here (minutes) in one place.
PRED_HORIZON_5MIN = int(os.getenv("PRED_HORIZON_5MIN", "5"))
PRED_HORIZON_4HOUR = int(os.getenv("PRED_HORIZON_4HOUR", "240"))
PRED_HORIZON_1DAY = int(os.getenv("PRED_HORIZON_1DAY", "1440"))
PRED_HORIZON_7DAY = int(os.getenv("PRED_HORIZON_7DAY", "10080"))

PREDICTION_TARGETS = {
    "5min_ahead": {"minutes": PRED_HORIZON_5MIN, "table": "waste_predictions_5min"},
    "4hour_ahead": {"minutes": PRED_HORIZON_4HOUR, "table": "waste_predictions_4hour"},
    "1day_ahead": {"minutes": PRED_HORIZON_1DAY, "table": "waste_predictions_1day"},
    "7day_ahead": {"minutes": PRED_HORIZON_7DAY, "table": "waste_predictions_7day"},
}

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


class WastePredictionManager:
    """
    Manages time-series predictions (ARIMA/Prophet) per bin.
    Maintains rolling 7-day history and retrains hourly.
    """
    def __init__(self):
        self.models = {}  # {bin_id: {'arima': model, 'data': list, 'last_train': datetime}}
        self.last_retraining = {}  # {bin_id: datetime}
        self.data_buffer = defaultdict(list)  # {bin_id: [(timestamp, fill), ...]}
        self.retrain_interval_seconds = 3600  # 1 hour
        
    def should_retrain(self, bin_id, current_time):
        """Check if model needs retraining (every hour)"""
        if bin_id not in self.last_retraining:
            return True
        return (current_time - self.last_retraining[bin_id]).total_seconds() > self.retrain_interval_seconds
    
    def add_data_point(self, bin_id, timestamp, fill):
        """Add a data point to the buffer"""
        self.data_buffer[bin_id].append((timestamp, fill))
        # Keep only 7 days of data
        cutoff = datetime.now() - timedelta(days=7)
        self.data_buffer[bin_id] = [(t, f) for t, f in self.data_buffer[bin_id] if t > cutoff]
    
    def train_model(self, bin_id):
        """
        Train lightweight time-series model on buffered data.
        Uses EMA + trend slope to avoid heavy native dependencies.
        """
        try:
            data = self.data_buffer.get(bin_id, [])
            if len(data) < 10:  # Need at least 10 points to train
                print(f"Not enough data for {bin_id} (only {len(data)} points)")
                return None
            
            sorted_data = sorted(data, key=lambda x: x[0])
            fill_values = [float(f) for _, f in sorted_data]

            alpha = 0.3
            ema = fill_values[0]
            for v in fill_values[1:]:
                ema = alpha * v + (1 - alpha) * ema

            count = len(fill_values)
            slope_per_point = 0.0 if count < 2 else (fill_values[-1] - fill_values[0]) / float(count - 1)

            intervals = []
            for i in range(1, len(sorted_data)):
                delta = (sorted_data[i][0] - sorted_data[i - 1][0]).total_seconds()
                if delta > 0:
                    intervals.append(delta)
            avg_interval_seconds = sum(intervals) / float(len(intervals)) if intervals else 2.0

            residuals = [abs(v - ema) for v in fill_values]
            mae = sum(residuals) / float(len(residuals))
            
            self.models[bin_id] = {
                'ema': ema,
                'slope_per_point': slope_per_point,
                'avg_interval_seconds': avg_interval_seconds,
                'data': fill_values,
                'last_train': datetime.now(),
                'mae': float(mae)
            }
            print(f"Trained model for {bin_id}: MAE={self.models[bin_id]['mae']:.2f}")
            return self.models[bin_id]
        except Exception as e:
            print(f"Error training model for {bin_id}: {str(e)}")
            return None
    
    def predict(self, bin_id):
        """
        Generate predictions for configured horizons using EMA + trend.
        Returns dict with predictions or None if model not ready.
        """
        if bin_id not in self.models:
            return None
        
        try:
            model_state = self.models[bin_id]
            current_fill = float(model_state['data'][-1])
            ema = float(model_state['ema'])
            slope_per_point = float(model_state['slope_per_point'])
            interval_seconds = max(float(model_state['avg_interval_seconds']), 1.0)
            mae = float(model_state.get('mae', 0.0))

            def forecast_fill(horizon_minutes):
                steps = max(int((horizon_minutes * 60.0) / interval_seconds), 1)
                trend_projection = current_fill + (slope_per_point * steps)
                blended = (0.6 * trend_projection) + (0.4 * ema)
                return max(0.0, min(100.0, blended))

            def forecast_confidence(horizon_minutes):
                horizon_penalty = min(horizon_minutes / 1440.0, 1.0) * 0.35
                error_penalty = min(mae / 100.0, 0.4)
                conf = 0.95 - horizon_penalty - error_penalty
                return max(0.3, min(0.95, conf))
            
            predictions = {}
            for horizon_key, cfg in PREDICTION_TARGETS.items():
                minutes = int(cfg["minutes"])
                predictions[horizon_key] = {
                    'value': forecast_fill(minutes),
                    'horizon_minutes': minutes,
                    'confidence': forecast_confidence(minutes),
                }
            
            return predictions
        except Exception as e:
            print(f"Error predicting for {bin_id}: {str(e)}")
            return None


def get_prediction_manager():
    """Global prediction manager instance"""
    if not hasattr(get_prediction_manager, '_instance'):
        get_prediction_manager._instance = WastePredictionManager()
    return get_prediction_manager._instance


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
        """
        Write raw data to PostgreSQL, generate predictions, and save predictions
        to both PostgreSQL and InfluxDB.
        """
        try:
            from datetime import datetime, timedelta
            
            postgres_url = f"jdbc:postgresql://{postgres_host}:{POSTGRES_PORT}/{POSTGRES_DB}"
            
            # Write raw waste stream data to PostgreSQL
            batch_df.write \
                .format("jdbc") \
                .option("url", postgres_url) \
                .option("dbtable", "waste_stream") \
                .option("user", POSTGRES_USER) \
                .option("password", POSTGRES_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            print(f"Batch {batch_id}: Raw data written to PostgreSQL")
            
            # Get prediction manager and process predictions
            pred_mgr = get_prediction_manager()
            current_time = datetime.now()
            prediction_records_by_table = defaultdict(list)
            latest_predictions = {}
            
            # Collect data for training (this simulates collecting from the batch)
            # In real scenario, you'd iterate through batch_df rows
            batch_list = batch_df.collect()
            
            for row in batch_list:
                bin_id = row.bin_id
                fill = float(row.fill)
                ts = current_time
                
                # Add to prediction manager's buffer
                pred_mgr.add_data_point(bin_id, ts, fill)
                
                # Retrain if needed (hourly)
                if pred_mgr.should_retrain(bin_id, ts):
                    trained = pred_mgr.train_model(bin_id)
                    if trained is not None:
                        pred_mgr.last_retraining[bin_id] = ts
                
                # Generate predictions from the latest state for this bin
                predictions = pred_mgr.predict(bin_id)
                if predictions:
                    latest_predictions[bin_id] = (ts, predictions)

            for bin_id, (ts, predictions) in latest_predictions.items():
                for horizon_key, pred_data in predictions.items():
                    table_name = PREDICTION_TARGETS[horizon_key]["table"]
                    predicted_fill = float(pred_data['value'])
                    confidence = float(pred_data['confidence'])
                    predicted_for = ts + timedelta(minutes=pred_data['horizon_minutes'])
                    prediction_records_by_table[table_name].append(
                        {
                            "bin_id": bin_id,
                            "predicted_at": ts,
                            "predicted_for": predicted_for,
                            "horizon_label": horizon_key,
                            "predicted_fill": predicted_fill,
                            "confidence": confidence,
                            "model_version": "EMA_TREND_V1",
                        }
                    )

            if latest_predictions:
                try:
                    from influxdb_client import InfluxDBClient, Point
                    from influxdb_client.client.write_api import SYNCHRONOUS

                    influx_client = InfluxDBClient(
                        url=INFLUXDB_URL,
                        token=INFLUXDB_TOKEN,
                        org=INFLUXDB_ORG,
                    )
                    influx_write_api = influx_client.write_api(write_options=SYNCHRONOUS)

                    for bin_id, (_, predictions) in latest_predictions.items():
                        for horizon_key, pred_data in predictions.items():
                            point = Point("waste_prediction") \
                                .tag("bin_id", bin_id) \
                                .tag("horizon", horizon_key) \
                                .field("predicted_fill", float(pred_data['value'])) \
                                .field("confidence", float(pred_data['confidence'])) \
                                .time(int(current_time.timestamp() * 1e9))
                            influx_write_api.write(bucket=INFLUXDB_BUCKET, record=point)

                    influx_client.close()
                    total_prediction_rows = sum(len(rows) for rows in prediction_records_by_table.values())
                    print(f"Batch {batch_id}: {total_prediction_rows} prediction rows written to InfluxDB")
                except Exception as e:
                    print(f"Error writing predictions to InfluxDB: {str(e)}")

            for table_name, records in prediction_records_by_table.items():
                if not records:
                    continue
                pred_df = batch_df.sparkSession.createDataFrame(records)
                pred_df.write \
                    .format("jdbc") \
                    .option("url", postgres_url) \
                    .option("dbtable", table_name) \
                    .option("user", POSTGRES_USER) \
                    .option("password", POSTGRES_PASSWORD) \
                    .option("driver", "org.postgresql.Driver") \
                    .mode("append") \
                    .save()
                print(f"Batch {batch_id}: {len(records)} prediction rows written to PostgreSQL table {table_name}")
        
        except Exception as e:
            print(f"Error processing batch {batch_id}: {str(e)}")

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