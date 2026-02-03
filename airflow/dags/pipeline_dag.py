from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

print("[bronze_to_silver] starting script...", flush=True)
import os
print("[bronze_to_silver] cluster_id=" + os.getenv("DATABRICKS_CLUSTER_ID", "MISSING"), flush=True)


# IMPORTANT:
# - Airflow container must have access to docker CLI + docker socket.
#   If you don't have it yet, we'll add it in the next step.
# - These commands run on the Docker host via docker CLI.

with DAG(
    dag_id="shot_pipeline_end_to_end",
    start_date=datetime(2026, 1, 1),
    schedule=None,          # manual trigger for now
    catchup=False,
    tags=["basketball", "kafka", "s3", "databricks"],
) as dag:

    # 1) Produce next game into Kafka (updates pipeline_state in MySQL)
    produce_next_game = BashOperator(
        task_id="produce_next_game",
        bash_command=(
            "docker run --rm "
            "--network basketball-shot-streaming-platform_default "
            "-e KAFKA_BOOTSTRAP=kafka:9092 "
            "-e KAFKA_TOPIC=shots_raw "
            "-e MYSQL_HOST=host.docker.internal "
            "-e MYSQL_PORT=8889 "
            "-e MYSQL_USER=root "
            "-e MYSQL_PASSWORD=root "
            "-e MYSQL_DATABASE=basketball_optimalization "
            "basketball-shot-streaming-platform-producer:latest"
        ),
    )

    # 2) Consume from Kafka and write ONE file per run to S3 bronze
    write_bronze_s3 = BashOperator(
        task_id="write_bronze_s3",
        bash_command=(
            "docker run --rm "
            "--network basketball-shot-streaming-platform_default "
            "-e KAFKA_BOOTSTRAP=kafka:9092 "
            "-e KAFKA_TOPIC=shots_raw "
            "-e KAFKA_GROUP_ID=bronze-s3-writer-{{ ts_nodash }} "  # <- důležité pro writer.py
            "-e AWS_REGION=eu-central-1 "          # <- writer.py chce AWS_REGION
            "-e S3_BUCKET=${S3_BUCKET} "           # <- musíš mít v airflow env_file nebo environment
            "-e AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} "
            "-e AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} "
            "-e S3_PREFIX=bronze/shots_raw "
            "basketball-shot-streaming-platform-bronze_s3_writer:latest"
        ),
    )

    # 3) Bronze -> Silver (Databricks Connect script)
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            "cd /opt/airflow/project && "
            "python -u databricks/jobs/bronze_to_silver.py"
        ),
    )

    # 4) Silver -> Gold
    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=(
            "cd /opt/airflow/project && "
            "python databricks/jobs/silver_to_gold.py"
        ),
    )

    produce_next_game >> write_bronze_s3 >> bronze_to_silver >> silver_to_gold
