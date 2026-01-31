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

# Initialize Spark session with serverless compute
spark = DatabricksSession.builder.serverless(True).getOrCreate()

BRONZE_PATH = "s3://basketball-shot-lakehouse-simon/bronze/shots_raw/"
SILVER_PATH = "s3://basketball-shot-lakehouse-simon/silver/shots_processed/"

# Define explicit schema for Bronze JSONL data to ensure type safety
bronze_schema = StructType([
    StructField("event_id", LongType(), True),
    StructField("game_id", LongType(), True),
    StructField("period", LongType(), True),
    StructField("player_id", LongType(), True),
    StructField("result", LongType(), True),  # 0 = miss, 1 = made
    StructField("score_after_shot", StringType(), True),  # Format: "XX - YY"
    StructField("score_diff_after", LongType(), True),
    StructField("score_diff_before", LongType(), True),
    StructField("shot_type", StringType(), True),  # "2pt" or "3pt"
    StructField("time", StringType(), True),  # Format: "MM:SS"
    StructField("x", DoubleType(), True),  # Court x-coordinate
    StructField("y", DoubleType(), True),  # Court y-coordinate
])

print("\nReading data from Bronze layer...")

df_bronze = spark.read \
    .schema(bronze_schema) \
    .json(BRONZE_PATH)

bronze_count = df_bronze.count()
print(f"Loaded {bronze_count:,} records from Bronze")

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

print("\nWriting to Silver layer...")

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("date") \
    .option("overwriteSchema", "true") \
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