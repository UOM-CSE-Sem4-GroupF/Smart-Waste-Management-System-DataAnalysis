import sys
from pyspark.sql import SparkSession

def test_db_connection():
    # Use localhost if running locally, 'postgres' if inside docker network
    jdbc_url = "jdbc:postgresql://localhost:5432/waste_management"
    properties = {
        "user": "waste_admin",
        "password": "waste_admin_password",
        "driver": "org.postgresql.Driver"
    }

    try:
        spark = SparkSession.builder \
            .appName("DBConnectivityTest") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
            .getOrCreate()

        print(f"Connecting to: {jdbc_url}")
        df = spark.read.jdbc(url=jdbc_url, table="bins", properties=properties)
        print(f"✅ Success! Bins count: {df.count()}")
        spark.stop()
        return True
    except Exception as e:
        print(f"❌ Failed: {e}")
        return False

if __name__ == "__main__":
    if test_db_connection(): sys.exit(0)
    else: sys.exit(1)
