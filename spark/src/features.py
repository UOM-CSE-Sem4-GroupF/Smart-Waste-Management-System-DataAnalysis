import logging
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

def engineering_fill_time(df_bins: DataFrame, df_current: DataFrame, df_cats: DataFrame) -> pd.DataFrame:
    """Feature engineering for fill-time prediction."""
    df = df_current.alias("cs") \
        .join(df_bins.alias("b"), F.col("cs.bin_id") == F.col("b.id")) \
        .join(df_cats.alias("wc"), F.col("b.waste_category_id") == F.col("wc.id"), "left") \
        .select(
            F.col("cs.fill_level_pct").cast("double"),
            F.col("cs.fill_rate_pct_per_hour").cast("double"),
            F.col("b.volume_litres").cast("double"),
            F.col("wc.avg_kg_per_litre").cast("double"),
            F.col("cs.urgency_score").cast("double"),
            F.hour("cs.last_reading_at").alias("hour").cast("double"),
            F.dayofweek("cs.last_reading_at").alias("dow").cast("double")
        )
    
    # Label: hours until full
    df = df.withColumn("label", 
        F.when(F.col("fill_rate_pct_per_hour") > 0, (100.0 - F.col("fill_level_pct")) / F.col("fill_rate_pct_per_hour"))
        .otherwise(0.0)
    )
    
    return df.dropna().toPandas()

def engineering_zone_gen(df_snapshots: DataFrame) -> pd.DataFrame:
    """Feature engineering for zone generation prediction."""
    if df_snapshots.rdd.isEmpty():
        return pd.DataFrame()
        
    window = Window.partitionBy("zone_id").orderBy("snapshot_at")
    df = df_snapshots.select(
        "snapshot_at",
        F.col("zone_id").cast("double"),
        F.col("avg_fill_level").cast("double"),
        F.col("total_estimated_kg").cast("double").alias("label"),
        F.col("urgent_bin_count").cast("double"),
        F.hour("snapshot_at").cast("double").alias("hour"),
        F.dayofweek("snapshot_at").cast("double").alias("dow")
    )
    df = df.withColumn("lag_1", F.lag("label", 1).over(window).cast("double"))
    return df.select("zone_id", "avg_fill_level", "urgent_bin_count", "hour", "dow", "lag_1", "label").dropna().toPandas()

def engineering_route_score(df_bins: DataFrame, df_current: DataFrame) -> pd.DataFrame:
    """Feature engineering for route score prediction (with augmentation)."""
    # Base real data
    df = df_current.join(df_bins, df_current.bin_id == df_bins.id) \
        .groupBy("zone_id") \
        .agg(
            F.count("*").alias("stops"),
            F.sum("estimated_weight_kg").alias("weight")
        )
    
    # Cast and calculate util/label
    df = df.select(
        F.col("stops").cast("double"),
        F.col("weight").cast("double"),
        F.lit(1500.0).alias("max_cargo")
    ).withColumn("util", F.col("weight") / F.col("max_cargo")) \
     .withColumn("label", F.greatest(F.lit(0.0), 100.0 - F.abs(0.78 - F.col("util")) * 120.0))
    
    pdf_real = df.select("stops", "weight", "max_cargo", "util", "label").toPandas()
    
    # Augmentation
    rng = np.random.default_rng(42)
    n = 100
    u = rng.uniform(0.4, 1.0, n)
    s = rng.uniform(10, 50, n)
    w = u * 1500.0
    l = np.clip(100.0 - np.abs(0.78 - u) * 120.0, 0, 100)
    
    pdf_aug = pd.DataFrame({"stops": s, "weight": w, "max_cargo": 1500.0, "util": u, "label": l})
    return pd.concat([pdf_real, pdf_aug])
