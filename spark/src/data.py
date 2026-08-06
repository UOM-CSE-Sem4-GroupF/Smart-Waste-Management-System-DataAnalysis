import logging
from pyspark.sql import SparkSession, DataFrame
from .config import JDBC_URL, JDBC_PROPERTIES

logger = logging.getLogger(__name__)

def load_table(spark: SparkSession, table: str) -> DataFrame:
    """Read a PostgreSQL table into a Spark DataFrame."""
    logger.info(f"Loading table: {table}")
    return spark.read.jdbc(url=JDBC_URL, table=table, properties=JDBC_PROPERTIES)

def save_to_db(df: DataFrame, table: str, mode: str = "overwrite"):
    """Write a Spark DataFrame to PostgreSQL."""
    logger.info(f"Saving to table: {table} with mode: {mode}")
    df.write.jdbc(url=JDBC_URL, table=table, mode=mode, properties=JDBC_PROPERTIES)
