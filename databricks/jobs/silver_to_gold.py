"""
Silver to Gold ETL Job
======================
Reads cleaned shot-level data from Silver layer and creates aggregated
analytical tables in Gold layer for business intelligence and reporting.

Silver: Cleaned, enriched Delta tables
Gold: Aggregated analytics, metrics, and KPIs
"""

from databricks.connect import DatabricksSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
import re
import boto3

# Initialize Spark session with serverless compute
spark = DatabricksSession.builder.serverless(True).getOrCreate()

SILVER_PATH = "s3://basketball-shot-lakehouse-simon/silver/shots_processed/"
GOLD_PATH = "s3://basketball-shot-lakehouse-simon/gold/"

def latest_silver_partition(silver_root: str) -> tuple[int, int]:
    """
    Finds latest (run_id, game_id) by scanning S3 keys:
    silver/shots_processed/game_id=1/run_id=17/...
    Returns (game_id, run_id)
    """
    m = re.match(r"^s3://([^/]+)/(.+)$", silver_root.rstrip("/") + "/")
    if not m:
        raise ValueError(f"Invalid S3 path: {silver_root}")
    bucket = m.group(1)
    prefix = m.group(2)

    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    best = None  # tuple (run_id, game_id)
    pat = re.compile(r"game_id=(\d+)/run_id=(\d+)/")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            m2 = pat.search(key)
            if not m2:
                continue
            gid = int(m2.group(1))
            rid = int(m2.group(2))
            if best is None or rid > best[0]:
                best = (rid, gid)

    if best is None:
        raise RuntimeError(f"No game_id/run_id partitions found under {silver_root}")

    rid, gid = best
    return gid, rid

def delete_partition_if_exists(delta_path: str, game_id: int, run_id: int):
    """
    Rerun-safe: remove existing rows for (game_id, run_id) if the delta table exists.
    Then we can append fresh results.
    """
    try:
        spark.read.format("delta").load(delta_path)  # table exists?
        spark.sql(f"DELETE FROM delta.`{delta_path}` WHERE game_id={game_id} AND run_id={run_id}")
    except Exception:
        # Table does not exist yet (first run) or DELETE not available -> ignore
        pass

latest_game_id, latest_run_id = latest_silver_partition(SILVER_PATH)

print("\nReading data from Silver layer...")

silver_partition_path = f"{SILVER_PATH}game_id={latest_game_id}/run_id={latest_run_id}/"
df_silver = spark.read.format("delta").load(silver_partition_path)

silver_count = df_silver.count()

print(f"Loaded {silver_count:,} records from Silver")

print("\nCreating Gold table: player_stats...")

df_player_stats = df_silver.groupBy("player_id", "game_id", "run_id") \
    .agg(
        # Shot counts
        F.count("*").alias("total_shots"),
        F.sum("shot_made").alias("made_shots"),
        
        # Shooting percentages
        F.avg("shot_made").alias("field_goal_percentage"),
        
        # Points
        F.sum("points_scored").alias("total_points"),
        
        # Distance metrics
        F.avg("shot_distance").alias("avg_shot_distance"),
        F.min("shot_distance").alias("min_shot_distance"),
        F.max("shot_distance").alias("max_shot_distance"),
        
        # 3-point stats
        F.sum(F.when(F.col("shot_type") == "3pt", 1).otherwise(0)).alias("three_point_attempts"),
        F.sum(F.when((F.col("shot_type") == "3pt") & (F.col("shot_made") == 1), 1).otherwise(0)).alias("three_point_made"),
        
        # 2-point stats
        F.sum(F.when(F.col("shot_type") == "2pt", 1).otherwise(0)).alias("two_point_attempts"),
        F.sum(F.when((F.col("shot_type") == "2pt") & (F.col("shot_made") == 1), 1).otherwise(0)).alias("two_point_made"),
        
        # Zone breakdown
        F.sum(F.when(F.col("shot_zone") == "paint", 1).otherwise(0)).alias("shots_in_paint"),
        F.sum(F.when(F.col("shot_zone") == "mid_range", 1).otherwise(0)).alias("shots_mid_range"),
        F.sum(F.when(F.col("shot_zone") == "three_point", 1).otherwise(0)).alias("shots_beyond_arc"),
        
        # Timestamp
        F.max("processing_timestamp").alias("last_updated")
    ) \
    .withColumn("missed_shots", F.col("total_shots") - F.col("made_shots")) \
    .withColumn("three_point_percentage", 
                F.when(F.col("three_point_attempts") > 0, 
                       F.col("three_point_made") / F.col("three_point_attempts") * 100)
                .otherwise(None)) \
    .withColumn("two_point_percentage",
                F.when(F.col("two_point_attempts") > 0,
                       F.col("two_point_made") / F.col("two_point_attempts") * 100)
                .otherwise(None)) \
    .withColumn("effective_fg_percentage",
                F.when(F.col("total_shots") > 0,
                       (F.col("made_shots") + 0.5 * F.col("three_point_made")) / F.col("total_shots") * 100)
                .otherwise(None))

# Write player_stats
player_stats_path = f"{GOLD_PATH}/player_stats/"
delete_partition_if_exists(player_stats_path, latest_game_id, latest_run_id)

df_player_stats.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("game_id", "run_id") \
    .save(player_stats_path)


player_stats_count = df_player_stats.count()
print(f"player_stats created: {player_stats_count:,} records")

print("\nCreating Gold table: game_stats...")

df_game_stats = df_silver.groupBy("game_id", "run_id") \
    .agg(
        F.count("*").alias("total_shots"),
        F.sum("shot_made").alias("total_made"),
        F.sum("points_scored").alias("total_points"),
        F.avg("shot_made").alias("overall_shooting_percentage"),
        F.avg("shot_distance").alias("avg_shot_distance"),
        F.countDistinct("player_id").alias("unique_players"),
        F.max("period").alias("periods_played"),
        
        # Shot type breakdown
        F.sum(F.when(F.col("shot_type") == "3pt", 1).otherwise(0)).alias("three_point_attempts"),
        F.sum(F.when(F.col("shot_type") == "2pt", 1).otherwise(0)).alias("two_point_attempts"),
        
        # Pace metrics
        F.count("*").alias("pace"),
        
        F.max("processing_timestamp").alias("last_updated")
    ) \
    .withColumn("shots_per_player", F.col("total_shots") / F.col("unique_players")) \
    .withColumn("three_point_rate", F.col("three_point_attempts") / F.col("total_shots") * 100)

# Write game_stats
game_stats_path = f"{GOLD_PATH}/game_stats/"
delete_partition_if_exists(game_stats_path, latest_game_id, latest_run_id)

df_game_stats.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("game_id", "run_id") \
    .save(game_stats_path)


game_stats_count = df_game_stats.count()
print(f"game_stats created: {game_stats_count:,} records")

print("\nCreating Gold table: shot_chart_data...")

# Detailed shot-level data with enrichment for visuals
df_shot_chart = df_silver.select(
    "player_id", "game_id", "run_id", "period",
    "x", "y", 
    "shot_distance", "shot_angle_degrees", "shot_zone",
    "shot_made", "shot_type", "points_scored",
    "time", "event_id"
) \
    .withColumn("shot_outcome", F.when(F.col("shot_made") == 1, "made").otherwise("missed"))

# Write shot_chart_data
shot_chart_path = f"{GOLD_PATH}/shot_chart_data/"
delete_partition_if_exists(shot_chart_path, latest_game_id, latest_run_id)

df_shot_chart.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("game_id", "run_id", "player_id") \
    .save(shot_chart_path)


shot_chart_count = df_shot_chart.count()
print(f"shot_chart_data created: {shot_chart_count:,} records")

print("\nCreating Gold table: temporal_trends...")

df_temporal = df_silver.groupBy("game_id", "run_id", "period") \
    .agg(
        F.count("*").alias("shots_count"),
        F.avg("shot_made").alias("shooting_percentage"),
        F.avg("shot_distance").alias("avg_distance"),
        F.sum("points_scored").alias("total_points"),
        F.sum(F.when(F.col("shot_type") == "3pt", 1).otherwise(0)).alias("three_point_attempts")
    ) \

# Write temporal_trends
temporal_path = f"{GOLD_PATH}/temporal_trends/"
delete_partition_if_exists(temporal_path, latest_game_id, latest_run_id)

df_temporal.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("game_id", "run_id") \
    .save(temporal_path)

temporal_count = df_temporal.count()
print(f"temporal_trends created: {temporal_count:,} records")

print("\nCreating Gold table: shot_zone_analysis...")

df_zone_analysis = df_silver.groupBy("player_id", "game_id", "run_id", "shot_zone") \
    .agg(
        F.count("*").alias("attempts"),
        F.sum("shot_made").alias("made"),
        F.avg("shot_made").alias("percentage"),
        F.sum("points_scored").alias("points"),
        F.avg("shot_distance").alias("avg_distance")
    ) \
    .withColumn("missed", F.col("attempts") - F.col("made"))

# Write shot_zone_analysis
zone_analysis_path = f"{GOLD_PATH}/shot_zone_analysis/"
delete_partition_if_exists(zone_analysis_path, latest_game_id, latest_run_id)

df_zone_analysis.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("game_id", "run_id") \
    .save(zone_analysis_path)


zone_analysis_count = df_zone_analysis.count()
print(f"shot_zone_analysis created: {zone_analysis_count:,} records")

print("\nCreating Gold table: player_performance_summary...")

# Aggregate across all games for each player
df_player_summary = df_silver.groupBy("player_id", "game_id", "run_id") \
    .agg(
        F.lit(1).alias("games_played"),
        F.count("*").alias("career_shots"),
        F.sum("shot_made").alias("career_made"),
        F.avg("shot_made").alias("career_fg_pct"),
        F.sum("points_scored").alias("career_points"),
        F.avg("shot_distance").alias("avg_career_distance"),
        
        # Best performances
        F.max(F.col("points_scored")).alias("best_single_shot_value"),
        
        F.max("processing_timestamp").alias("last_updated")
    ) \
    .withColumn("shots_per_game", F.col("career_shots") / F.col("games_played")) \
    .withColumn("points_per_game", F.col("career_points") / F.col("games_played"))

# Write player_performance_summary
player_summary_path = f"{GOLD_PATH}/player_performance_summary/"

# rerun-safe for game_id + run_id
try:
    spark.read.format("delta").load(player_summary_path)
    spark.sql(f"DELETE FROM delta.`{player_summary_path}` WHERE game_id={latest_game_id} AND run_id={latest_run_id}")
except Exception:
    pass

df_player_summary.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("game_id", "run_id") \
    .save(player_summary_path)



player_summary_count = df_player_summary.count()
print(f"player_performance_summary created: {player_summary_count:,} records")

gold_tables = [
    ("player_stats", player_stats_path, player_stats_count),
    ("game_stats", game_stats_path, game_stats_count),
    ("shot_chart_data", shot_chart_path, shot_chart_count),
    ("temporal_trends", temporal_path, temporal_count),
    ("shot_zone_analysis", zone_analysis_path, zone_analysis_count),
    ("player_performance_summary", player_summary_path, player_summary_count),
]

# Clean up
spark.stop()
print("\nSilver to Gold aggregation completed successfully!")