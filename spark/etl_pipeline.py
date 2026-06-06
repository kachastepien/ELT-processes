# UX Analytics ETL - conversion funnel
#
# Goal: find where users drop off in the funnel and which device converts best.
# Layers: bronze (raw json) -> silver (cleaned) -> gold (aggregated metrics).

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, BooleanType, LongType
)
import os

BASE_DIR  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR   = os.path.join(BASE_DIR, "data", "raw")
CLEAN_DIR = os.path.join(BASE_DIR, "data", "clean")
GOLD_DIR  = os.path.join(BASE_DIR, "data", "gold")

spark = (
    SparkSession.builder
    .appName("UXAnalytics-ETL")
    .master("local[*]")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


# BRONZE - load raw data as-is
def load_bronze():
    print("\n=== BRONZE: Loading raw data ===")

    session_schema = StructType([
        StructField("session_id",   StringType(),  True),
        StructField("user_id",      StringType(),  True),
        StructField("page",         StringType(),  True),
        StructField("device",       StringType(),  True),
        StructField("duration_sec", LongType(),    True),
        StructField("clicked_cta",  BooleanType(), True),
        StructField("converted",    BooleanType(), True),
        StructField("ts",           StringType(),  True),
        StructField("country",      StringType(),  True),
    ])

    page_schema = StructType([
        StructField("page_id",      IntegerType(), True),
        StructField("page",         StringType(),  True),
        StructField("funnel_step",  IntegerType(), True),
        StructField("step_name",    StringType(),  True),
    ])

    df_sessions = spark.read.schema(session_schema).json(
        os.path.join(RAW_DIR, "sessions.json")
    )
    df_pages = spark.read.schema(page_schema).json(
        os.path.join(RAW_DIR, "pages.json")
    )

    print(f"  Sessions (raw) : {df_sessions.count()} rows")
    print(f"  Pages    (raw) : {df_pages.count()} rows")
    df_sessions.show(truncate=False)
    return df_sessions, df_pages


# SILVER - clean & validate
def clean_sessions(df_raw):
    """
    Silver layer – sessions.
    Data quality rules applied:
      DQ-1  Drop rows where session_id OR user_id is null (primary key)
      DQ-2  Normalize device to lowercase (avoids 'MOBILE' vs 'mobile' split)
      DQ-3  Nullify duration_sec < 0 or > 7200 (measurement error / bots)
      DQ-4  Safe timestamp parsing — bad format -> NULL, never crashes job
      DQ-5  Fill missing country with 'UNKNOWN'
      DQ-6  Flag suspicious sessions: duration < 5 sec (likely bot traffic)
    """
    print("\n=== SILVER: Cleaning sessions ===")

    # DQ-1: Drop missing primary keys
    df = df_raw.dropna(subset=["session_id", "user_id"])
    dropped = df_raw.count() - df.count()
    print(f"  DQ-1: Dropped {dropped} rows with null session_id/user_id")

    # DQ-2: Normalize device
    df = df.withColumn("device", F.lower(F.trim(F.col("device"))))

    # DQ-3: Sanitize duration
    df = df.withColumn(
        "duration_sec",
        F.when(
            (F.col("duration_sec") < 0) | (F.col("duration_sec") > 7200),
            F.lit(None)
        ).otherwise(F.col("duration_sec"))
    )

    # DQ-4: Safe timestamp parse
    df = df.withColumn(
        "event_ts",
        F.expr("try_to_timestamp(ts, 'yyyy-MM-dd HH:mm:ss')")
    )

    # DQ-5: Fill missing country
    df = df.withColumn(
        "country",
        F.when(F.col("country").isNull(), F.lit("UNKNOWN"))
         .otherwise(F.upper(F.col("country")))
    )

    # DQ-6: Bot/suspicious flag
    df = df.withColumn(
        "is_suspicious",
        F.col("duration_sec") < 5
    )

    # Audit columns
    df = (df
          .withColumn("_layer",       F.lit("silver"))
          .withColumn("_loaded_at",   F.current_timestamp()))

    print(f"  Silver sessions: {df.count()} rows")
    df.show(truncate=False)
    return df


def clean_pages(df_raw):
    print("\n=== SILVER: Cleaning pages ===")
    df = df_raw.dropna(subset=["page_id"])
    df = (df
          .withColumn("step_name", F.trim(F.col("step_name")))
          .withColumn("_layer",    F.lit("silver")))
    print(f"  Silver pages: {df.count()} rows")
    df.show()
    return df


# GOLD - aggregations + joins
def build_gold(df_sessions, df_pages):
    """
    Gold layer – two tables:
      1. funnel_metrics    – conversion rate per funnel step (JOIN + AGG)
      2. device_metrics    – CTR and CVR breakdown by device type (AGG)
    """
    print("\n=== GOLD: Building analytics tables ===")

    # gold 1: funnel conversion metrics (join + agg)
    funnel_metrics = (
        df_sessions
        .join(
            df_pages.select("page", "funnel_step", "step_name"),
            on="page", how="left"
        )
        .groupBy("page", "funnel_step", "step_name")
        .agg(
            F.count("session_id").alias("session_count"),
            F.round(F.avg("duration_sec"), 1).alias("avg_duration_sec"),
            F.sum(F.col("clicked_cta").cast("int")).alias("cta_clicks"),
            F.sum(F.col("converted").cast("int")).alias("conversions"),
            F.round(
                F.sum(F.col("converted").cast("int")) * 100.0
                / F.count("session_id"), 1
            ).alias("conversion_rate_pct"),
        )
        .orderBy("funnel_step")
    )

    print("\n  [GOLD] funnel_metrics:")
    funnel_metrics.show(truncate=False)

    # gold 2: device breakdown
    device_metrics = (
        df_sessions
        .groupBy("device")
        .agg(
            F.count("session_id").alias("session_count"),
            F.round(F.avg("duration_sec"), 1).alias("avg_duration_sec"),
            F.round(
                F.sum(F.col("clicked_cta").cast("int")) * 100.0
                / F.count("session_id"), 1
            ).alias("cta_ctr_pct"),
            F.round(
                F.sum(F.col("converted").cast("int")) * 100.0
                / F.count("session_id"), 1
            ).alias("cvr_pct"),
        )
        .orderBy("session_count", ascending=False)
    )

    print("\n  [GOLD] device_metrics:")
    device_metrics.show(truncate=False)

    return funnel_metrics, device_metrics


# save to parquet
def save(df, path, label):
    df.write.mode("overwrite").parquet(path)
    print(f"  Saved {label} -> {path}  ({df.count()} rows)")


def run():
    print("=" * 60)
    print("  UX Analytics ETL Pipeline")
    print("  Goal: Identify conversion funnel bottlenecks")
    print("=" * 60)

    df_sessions_raw, df_pages_raw = load_bronze()

    df_sessions_clean = clean_sessions(df_sessions_raw)
    df_pages_clean    = clean_pages(df_pages_raw)

    save(df_sessions_clean, os.path.join(CLEAN_DIR, "sessions"), "silver/sessions")
    save(df_pages_clean,    os.path.join(CLEAN_DIR, "pages"),    "silver/pages")

    funnel_metrics, device_metrics = build_gold(df_sessions_clean, df_pages_clean)

    save(funnel_metrics,  os.path.join(GOLD_DIR, "funnel_metrics"),  "gold/funnel_metrics")
    save(device_metrics,  os.path.join(GOLD_DIR, "device_metrics"),  "gold/device_metrics")

    print("\nPipeline complete.")
    spark.stop()


if __name__ == "__main__":
    run()
