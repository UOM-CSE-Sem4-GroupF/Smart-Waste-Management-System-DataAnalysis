from pyspark.sql import SparkSession

#spark session
spark = SparkSession.builder \
    .appName("WasteAnalyticsJob") \
    .getOrCreate()

#pg connection
jdbc_url = "jdbc:postgresql://postgres:5432/waste_management"

properties = {
    "user": "waste_admin",
    "password": "waste_admin_password",
    "driver": "org.postgresql.Driver"
}

#read tables from pg
df_bins = spark.read.jdbc(
    url=jdbc_url,
    table="bins",
    properties=properties
)

df_current = spark.read.jdbc(
    url=jdbc_url,
    table="bin_current_state",
    properties=properties
)

#join to get zone_id
df = df_current.join(df_bins, df_current.bin_id == df_bins.id, "inner") \
               .select(df_current["*"], df_bins["zone_id"])

print("=== Raw Data ===")
df.show()

#avarage weight per zone 
result = df.groupBy("zone_id") \
           .avg("estimated_weight_kg") \
           .withColumnRenamed("avg(estimated_weight_kg)", "avg_weight")

print("=== Average Weight per Zone ===")
result.show()

# Write back to PostgreSQL
result.write.jdbc(
    url=jdbc_url,
    table="zone_avg_weight",
    mode="overwrite",
    properties=properties
)

print("=== Verification: Reading back from zone_avg_weight table ===")
try:
    verification_df = spark.read.jdbc(
        url=jdbc_url,
        table="zone_avg_weight",
        properties=properties
    )
    verification_df.show()
    print("✅ Successfully wrote and read back zone averages!")
except Exception as e:
    print(f"❌ Error reading back data: {e}")

spark.stop()
