"""
Bronze to Silver ETL Job
========================
Reads raw shot-level events from Bronze layer (S3 JSONL files),
performs data cleaning, validation, enrichment, and writes cleaned data
to Silver layer as Delta tables.

Bronze: Raw JSONL data from Kafka stream
Silver: Cleaned, validated, enriched Delta tables
"""

from databricks.connect import DatabricksSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType
from datetime import datetime
import re
import boto3
from pyspark.sql.utils import AnalysisException

# Initialize Spark session with serverless compute
spark = DatabricksSession.builder.serverless(True).getOrCreate()

BRONZE_ROOT = "s3://basketball-shot-lakehouse-simon/bronze/shots_raw/"
SILVER_PATH = "s3://basketball-shot-lakehouse-simon/silver/shots_processed/"

# Define explicit schema for Bronze JSONL data to ensure type safety
bronze_schema = StructType([
    StructField("type", StringType(), True),
    StructField("run_id", LongType(), True),
    StructField("event_id", LongType(), True),
    StructField("game_id", LongType(), True),
    StructField("period", LongType(), True),
    StructField("player_id", LongType(), True),
    StructField("result", LongType(), True),
    StructField("score_after_shot", StringType(), True),
    StructField("score_diff_after", LongType(), True),
    StructField("score_diff_before", LongType(), True),
    StructField("shot_type", StringType(), True),
    StructField("time", StringType(), True),
    StructField("x", DoubleType(), True),
    StructField("y", DoubleType(), True),
    StructField("emitted_at", StringType(), True),
])

def latest_run_path(bronze_root: str) -> str:
    """
    Terminal-friendly: find latest run_id in S3 using boto3 list_objects_v2.

    Expects keys like:
    bronze/shots_raw/game_id=1/run_id=17/date=2026-02-02/shots.jsonl

    Returns:
    s3://bucket/bronze/shots_raw/game_id=<gid>/run_id=<rid>/
    """
    m = re.match(r"^s3://([^/]+)/(.+)$", bronze_root.rstrip("/") + "/")
    if not m:
        raise ValueError(f"Invalid S3 path: {bronze_root}")
    bucket = m.group(1)
    prefix = m.group(2)

    s3 = boto3.client("s3")

    paginator = s3.get_paginator("list_objects_v2")

    best = None  # tuple (run_id, game_id)

    run_re = re.compile(r"game_id=(\d+)/run_id=(\d+)/")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            m2 = run_re.search(key)
            if not m2:
                continue
            gid = int(m2.group(1))
            rid = int(m2.group(2))
            if best is None or rid > best[0]:
                best = (rid, gid)

    if best is None:
        raise RuntimeError(f"No run_id found under {bronze_root}")

    rid, gid = best
    return f"s3://{bucket}/{prefix}game_id={gid}/run_id={rid}/"

print("\n[bronze_to_silver] Discovering latest run in Bronze...")
BRONZE_PATH = latest_run_path(BRONZE_ROOT)
print(f"[bronze_to_silver] Latest run path: {BRONZE_PATH}")

print("\n[bronze_to_silver] Reading Bronze JSONL...")
df_bronze = (
    spark.read
        .schema(bronze_schema)
        .json(BRONZE_PATH)
        .filter(F.col("type") == F.lit("SHOT"))
)

bronze_count = df_bronze.count()
print(f"[bronze_to_silver] Loaded {bronze_count:,} SHOT records from Bronze")

print("[bronze_to_silver] Distinct run/game/date found:")
df_bronze.select("run_id", "game_id").dropDuplicates().show(truncate=False)

df_bronze.printSchema()

# Apply transformations
df_silver = df_bronze \
    .filter(F.col("result").isNotNull()) \
    .filter(F.col("x").isNotNull()) \
    .filter(F.col("y").isNotNull()) \
    .filter(F.col("event_id").isNotNull()) \
    .filter(F.col("game_id").isNotNull()) \
    .dropDuplicates(["event_id", "game_id"]) \
    .withColumn("shot_distance", 
                F.sqrt(F.col("x")**2 + F.col("y")**2)) \
    .withColumn("is_three_pointer", 
                F.when(F.col("shot_type") == "3pt", 1).otherwise(0)) \
    .withColumn("shot_angle_degrees", 
                F.atan2(F.col("y"), F.col("x")) * 180 / F.lit(3.14159265359)) \
    .withColumn("shot_made", 
                F.col("result").cast("int")) \
    .withColumn("shot_value", 
                F.when(F.col("shot_type") == "3pt", 3).otherwise(2)) \
    .withColumn("points_scored", 
                F.col("shot_made") * F.col("shot_value")) \
    .withColumn("processing_timestamp", 
                F.current_timestamp()) \
    .withColumn("shot_zone", 
                F.when(F.col("shot_distance") < 10, "paint")
                 .when(F.col("shot_distance") < 23.75, "mid_range")
                 .otherwise("three_point")) \
    .withColumn("quarter", F.col("period"))

# Add data quality flags
df_silver = df_silver \
    .withColumn("is_valid_coordinates", 
                (F.col("x").between(0, 100)) & (F.col("y").between(0, 100))) \
    .withColumn("is_valid_shot_type",
                F.col("shot_type").isin(["2pt", "3pt"]))

silver_count = df_silver.count()
records_removed = bronze_count - silver_count

print(f"Transformations applied")
print(f"\tRecords after cleaning: {silver_count:,}")
print(f"\tRecords removed: {records_removed:,} ({records_removed/bronze_count*100:.2f}%)")

# Check for invalid coordinates
invalid_coords = df_silver.filter(F.col("is_valid_coordinates") == False).count()
print(f"\tInvalid coordinates: {invalid_coords}")

# Check for invalid shot types
invalid_shot_types = df_silver.filter(F.col("is_valid_shot_type") == False).count()
print(f"\tInvalid shot types: {invalid_shot_types}")

# Show distribution by shot type
print("\nShot type distribution:")
df_silver.groupBy("shot_type", "shot_made") \
    .count() \
    .orderBy("shot_type", "shot_made") \
    .show()
    
try:
    spark.read.format("delta").load(SILVER_PATH)
    run_meta = df_silver.select("game_id", "run_id").dropDuplicates().collect()
    game_id = int(run_meta[0]["game_id"])
    run_id = int(run_meta[0]["run_id"])
    spark.sql(f"DELETE FROM delta.`{SILVER_PATH}` WHERE game_id = {game_id} AND run_id = {run_id}")
except Exception:
    pass

print("\nWriting to Silver layer...")


df_silver.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("game_id", "run_id") \
    .option("mergeSchema", "true") \
    .save(SILVER_PATH)

print(f"Silver layer created successfully!")

# Verify silver data
df_verify = spark.read.format("delta").load(SILVER_PATH)
verify_count = df_verify.count()

print(f"'\tVerification count: {verify_count:,}")
print(f"\tMatch: {'YES' if verify_count == silver_count else 'NO'}")

# Clean up
spark.stop()
print("\nBronze to Silver transformation completed successfully!")