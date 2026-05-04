from __future__ import annotations

from pyspark.sql import SparkSession


def process_data(df_bins, df_current):
    """Join current readings with bin metadata and compute average weight per zone."""
    joined = df_current.join(df_bins, df_current.bin_id == df_bins.id, "inner").select(
        df_current["*"], df_bins["zone_id"]
    )
    return joined.groupBy("zone_id").avg("estimated_weight_kg").withColumnRenamed(
        "avg(estimated_weight_kg)", "avg_weight"
    )


def run_job() -> None:
    spark = SparkSession.builder.appName("WasteAnalyticsJob").getOrCreate()

    jdbc_url = "jdbc:postgresql://postgres:5432/waste_management"
    properties = {
        "user": "waste_admin",
        "password": "waste_admin_password",
        "driver": "org.postgresql.Driver",
    }

    df_bins = spark.read.jdbc(url=jdbc_url, table="bins", properties=properties)
    df_current = spark.read.jdbc(url=jdbc_url, table="bin_current_state", properties=properties)

    result = process_data(df_bins, df_current)

    result.write.jdbc(
        url=jdbc_url,
        table="zone_avg_weight",
        mode="overwrite",
        properties=properties,
    )

    spark.stop()


if __name__ == "__main__":
    run_job()
