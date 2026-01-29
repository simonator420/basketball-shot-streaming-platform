import os
import json
import time
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

# Batching & flushing configuration
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
FLUSH_SECONDS = int(os.getenv("FLUSH_SECONDS", "10"))


def validate_env():
    """
    Validate that required environment variables are set.
    Fail fast if critical AWS configuration is missing.
    """
    missing = []
    for k in ["AWS_REGION", "S3_BUCKET"]:
        if not os.getenv(k):
            missing.append(k)
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")


def make_s3_key():
    """
    Generate an S3 object key using UTC timestamp.
    The structure follows a bronze data lake layout with date partitioning.

    Example:
    bronze/shots_raw/date=2026-01-28/part-20260128_120501_123456.jsonl
    """
    dt = datetime.now(timezone.utc)
    date = dt.strftime("%Y-%m-%d")
    ts = dt.strftime("%Y%m%d_%H%M%S_%f")
    return f"{S3_PREFIX}/date={date}/part-{ts}.jsonl"


def main():
    """
    Main consumer loop:
    - Read messages from Kafka
    - Buffer them in memory
    - Periodically flush them to S3 as JSON Lines (NDJSON)
    - Commit Kafka offsets only after successful S3 upload
    """
    
    # Ensure required environment variables are present
    validate_env()

    # Initialize S3 client
    s3 = boto3.client("s3", region_name=AWS_REGION)

    # Initialize Kafka consumer
    c = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        # Start from earliest offset if no committed offset exists
        "auto.offset.reset": AUTO_OFFSET_RESET,
        # Disable auto commit to control delivery guarantees
        "enable.auto.commit": False,
    })
    
    # Subscribe to the Kafka topic
    c.subscribe([TOPIC])

    # In-memory buffer for batching messages before upload
    buffer = []
    last_flush = time.time()
    
    # Startup logs
    print(f"[bronze-s3-writer] Kafka={KAFKA_BOOTSTRAP} topic={TOPIC} group={GROUP_ID}")
    print(f"[bronze-s3-writer] S3=s3://{S3_BUCKET}/{S3_PREFIX}/... region={AWS_REGION}")
    print(f"[bronze-s3-writer] batch={BATCH_SIZE} flush_seconds={FLUSH_SECONDS}")

    try:
        while True:
            # Poll Kafka for new messages
            msg = c.poll(1.0)
            now = time.time()

            if msg is not None:
                if msg.error():
                    print(f"Kafka error: {msg.error()}")
                else:
                    # Decode message payload
                    raw = msg.value().decode("utf-8")
                    
                    # Sanity check, ensure valid JSON
                    json.loads(raw)
                    
                    # Add raw JSON string to buffer
                    buffer.append(raw)

            # Decide whether to flush buffer to S3:
            # 1) buffer size threshold reached OR
            # 2) time-based flush interval exceeded
            should_flush = (len(buffer) >= BATCH_SIZE) or (buffer and (now - last_flush) >= FLUSH_SECONDS)

            if should_flush:
                # Generate unique S3 object key
                key = make_s3_key()
                
                # Join messages as newline-delimited JSON
                body = ("\n".join(buffer) + "\n").encode("utf-8")

                # Upload batch to S3
                s3.put_object(
                    Bucket=S3_BUCKET,
                    Key=key,
                    Body=body,
                    ContentType="application/x-ndjson",
                )

                # Commit offsets only after successful upload
                c.commit(asynchronous=False)

                print(f"Uploaded {len(buffer)} msgs -> s3://{S3_BUCKET}/{key} and committed offsets.")
                
                # Reset buffer and flush timer
                buffer.clear()
                last_flush = now

    except KeyboardInterrupt:
        print("Stopping...")

    finally:
        # Best-effort flush of remaining messages on shutdown
        if buffer:
            try:
                # Generate a unique S3 object key
                key = make_s3_key()
                # Convert buffered Kafka messages
                body = ("\n".join(buffer) + "\n").encode("utf-8")
                # Upload the batch to S3 as a single object
                # This represents durable storage in the bronze data layer
                s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body, ContentType="application/x-ndjson")
                # Commit Kafka offsets only after successful upload to S3
                c.commit(asynchronous=False)
                print(f"Uploaded final {len(buffer)} msgs -> s3://{S3_BUCKET}/{key} and committed offsets.")
            except Exception as e:
                print(f"Final flush failed: {e}")
        # Close Kafka consumer cleanly
        c.close()
        print("Done")

if __name__ == "__main__":
    main()
