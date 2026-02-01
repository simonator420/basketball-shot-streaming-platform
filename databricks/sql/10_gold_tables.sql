CREATE TABLE IF NOT EXISTS workspace_ingestion_data.analytics.player_stats
USING DELTA
LOCATION 's3://basketball-shot-lakehouse-simon/gold/player_stats/';

CREATE TABLE IF NOT EXISTS workspace_ingestion_data.analytics.game_stats
USING DELTA
LOCATION 's3://basketball-shot-lakehouse-simon/gold/game_stats/';

CREATE TABLE IF NOT EXISTS workspace_ingestion_data.analytics.shot_chart_data
USING DELTA
LOCATION 's3://basketball-shot-lakehouse-simon/gold/shot_chart_data/';

CREATE TABLE IF NOT EXISTS workspace_ingestion_data.analytics.temporal_trends
USING DELTA
LOCATION 's3://basketball-shot-lakehouse-simon/gold/temporal_trends/';

CREATE TABLE IF NOT EXISTS workspace_ingestion_data.analytics.shot_zone_analysis
USING DELTA
LOCATION 's3://basketball-shot-lakehouse-simon/gold/shot_zone_analysis/';

CREATE TABLE IF NOT EXISTS workspace_ingestion_data.analytics.player_performance_summary
USING DELTA
LOCATION 's3://basketball-shot-lakehouse-simon/gold/player_performance_summary/';
