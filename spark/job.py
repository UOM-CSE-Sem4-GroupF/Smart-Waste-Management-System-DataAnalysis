import logging
import sys
from pyspark.sql import SparkSession
import mlflow

from src.config import MODEL_FILL_TIME, MODEL_ZONE_GEN, MODEL_ROUTE_SCORE
from src.data import load_table, save_to_db
from src.features import engineering_fill_time, engineering_zone_gen, engineering_route_score
from src.models import train_fill_time, train_zone_gen, train_route_score
from src.tracking import init_mlflow, log_model

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("WasteMLJob")

def run_job():
    logger.info("Starting Waste Management ML Training Job")
    
    spark = SparkSession.builder \
        .appName("WasteMLTrainingJob") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()

    try:
        # 1. Load Data
        df_bins = load_table(spark, "bins")
        df_current = load_table(spark, "bin_current_state")
        df_cats = load_table(spark, "waste_categories")
        df_snapshots = load_table(spark, "zone_snapshots")

        # 2. Legacy Analytics (Keep leadership happy)
        from src.data import JDBC_URL, JDBC_PROPERTIES
        joined = df_current.join(df_bins, df_current.bin_id == df_bins.id)
        zone_avg = joined.groupBy("zone_id").avg("estimated_weight_kg").withColumnRenamed("avg(estimated_weight_kg)", "avg_weight")
        save_to_db(zone_avg, "zone_avg_weight")

        # 3. Feature Engineering
        logger.info("Engineering features...")
        pdf_fill = engineering_fill_time(df_bins, df_current, df_cats)
        pdf_zone = engineering_zone_gen(df_snapshots)
        pdf_route = engineering_route_score(df_bins, df_current)

        # 4. MLflow Init
        init_mlflow()

        with mlflow.start_run(run_name="Main_Training_Run"):
            # 5. Train & Log Models
            logger.info("Training Fill Time Model...")
            m_fill, met_fill, p_fill = train_fill_time(pdf_fill)
            if m_fill: log_model(m_fill, MODEL_FILL_TIME, met_fill, p_fill)

            logger.info("Training Zone Generation Model...")
            m_zone, met_zone, p_zone = train_zone_gen(pdf_zone)
            if m_zone: log_model(m_zone, MODEL_ZONE_GEN, met_zone, p_zone)

            logger.info("Training Route Score Model...")
            m_route, met_route, p_route = train_route_score(pdf_route)
            if m_route: log_model(m_route, MODEL_ROUTE_SCORE, met_route, p_route)

        logger.info("ML Job Completed Successfully")

    except Exception as e:
        logger.error(f"Job failed: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    run_job()
