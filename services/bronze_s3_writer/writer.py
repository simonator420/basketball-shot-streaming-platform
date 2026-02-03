import os
import json
from datetime import datetime, timezone

import boto3
from confluent_kafka import Consumer

# Kafka connection settings
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "shots_raw")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "bronze-s3-writer")
AUTO_OFFSET_RESET = os.getenv("AUTO_OFFSET_RESET", "earliest")

# AWS / S3 configuration
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = os.getenv("S3_PREFIX", "bronze/shots_raw")


def validate_env():
    missing = []
    for k in ["AWS_REGION", "S3_BUCKET"]:
        if not os.getenv(k):
            missing.append(k)
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")


def make_s3_key(game_id: int, run_id: int):
    """
    One file per run (per game).
    Example:
    bronze/shots_raw/game_id=1/run_id=17/date=2026-02-02/shots.jsonl
    """
    dt = datetime.now(timezone.utc)
    date = dt.strftime("%Y-%m-%d")
    return f"{S3_PREFIX}/game_id={game_id}/run_id={run_id}/date={date}/shots.jsonl"


def main():
    validate_env()

    s3 = boto3.client("s3", region_name=AWS_REGION)

    c = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": AUTO_OFFSET_RESET,
        "enable.auto.commit": False,
    })

    c.subscribe([TOPIC])

    print(f"[bronze-s3-writer] Kafka={KAFKA_BOOTSTRAP} topic={TOPIC} group={GROUP_ID}")
    print(f"[bronze-s3-writer] S3=s3://{S3_BUCKET}/{S3_PREFIX}/... region={AWS_REGION}")
    print("[bronze-s3-writer] Mode=ONE_FILE_PER_RUN (upload on RUN_END)")

    current_run_id = None
    current_game_id = None
    shots_written = 0
    tmp_path = None
    f = None

    try:
        while True:
            msg = c.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                print(f"Kafka error: {msg.error()}")
                continue

            raw = msg.value().decode("utf-8")
            event = json.loads(raw)
            etype = event.get("type", "SHOT")

            if etype == "SHOT":
                if current_run_id is None:
                    current_run_id = int(event["run_id"])
                    current_game_id = int(event["game_id"])

                    # local temp file for this run
                    tmp_path = f"/tmp/shots_run_{current_run_id}.jsonl"
                    f = open(tmp_path, "w", encoding="utf-8")

                    print(f"[bronze-s3-writer] Started run_id={current_run_id} game_id={current_game_id}")
                    print(f"[bronze-s3-writer] Writing temp file: {tmp_path}")

                # Fail fast if mixed runs arrive (should not happen in this design)
                if int(event["run_id"]) != current_run_id:
                    raise RuntimeError(
                        f"Received run_id={event['run_id']} but current_run_id={current_run_id}. "
                        "Mixed runs in one writer execution are not supported."
                    )

                # Write exactly what came from Kafka as NDJSON line
                f.write(raw + "\n")
                shots_written += 1

            elif etype == "RUN_END":
                rid = int(event["run_id"])
                gid = int(event["game_id"])
                expected = int(event.get("shots_sent", -1))

                # If RUN_END comes first, initialize anyway (rare)
                if current_run_id is None:
                    current_run_id = rid
                    current_game_id = gid
                    tmp_path = f"/tmp/shots_run_{current_run_id}.jsonl"
                    f = open(tmp_path, "w", encoding="utf-8")

                if rid != current_run_id:
                    raise RuntimeError(f"RUN_END run_id={rid} != current_run_id={current_run_id}")

                print(f"[bronze-s3-writer] RUN_END received run_id={rid} expected={expected} written={shots_written}")

                # Close file before upload
                f.flush()
                f.close()

                # Upload exactly one object for this run
                key = make_s3_key(current_game_id, current_run_id)
                with open(tmp_path, "rb") as rf:
                    s3.put_object(
                        Bucket=S3_BUCKET,
                        Key=key,
                        Body=rf,
                        ContentType="application/x-ndjson",
                    )

                # Commit offsets only after successful upload
                c.commit(asynchronous=False)

                print(f"[bronze-s3-writer] Uploaded ONE file -> s3://{S3_BUCKET}/{key}")
                print("[bronze-s3-writer] Offsets committed. Exiting.")

                break

    except KeyboardInterrupt:
        print("Stopping...")

    finally:
        try:
            if f and not f.closed:
                f.close()
        except Exception:
            pass

        # Best-effort cleanup temp file
        try:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass

        c.close()
        print("Done")


if __name__ == "__main__":
    main()
