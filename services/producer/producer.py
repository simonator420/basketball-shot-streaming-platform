import os
import json
from dotenv import load_dotenv
from confluent_kafka import Producer
import mysql.connector

# Load environment variables from .env file
load_dotenv()

def env_int(key: str, default: int) -> int:
    """
    Read an integer environment variable with a fallback default value.
    """
    v = os.getenv(key, str(default))
    return int(v)

def delivery_report(err, msg):
    """
    Delivery callback for Kafka producer.
    Called once a message has been successfully delivered or failed.
    """
    if err is not None:
        print(f"Delivery failed: {err}")
    # else:
    #     print(f"Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

def main():
    """
    Extract data from MySQL and publish it as JSON events to a Kafka topic.
    This acts as the ingestion (producer) component of the pipeline.
    """
    
    # Kafka configuration
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC", "shots_raw")

    # MySQL configuration
    mysql_host = os.getenv("MYSQL_HOST", "127.0.0.1")
    mysql_port = env_int("MYSQL_PORT", 8889)
    mysql_user = os.getenv("MYSQL_USER", "root")
    mysql_password = os.getenv("MYSQL_PASSWORD", "")
    mysql_db = os.getenv("MYSQL_DATABASE", "basketball_optimalization")

    limit_rows = env_int("LIMIT_ROWS", 100)

    # Query retrieves shot-level events ordered for deterministic streaming
    query = f"""
    SELECT
      id, game_id, player_id, result, x, y, shot_type, period, time,
      score_after_shot, score_differential_after_shot, score_differential_before_shot
    FROM Shots
    ORDER BY game_id, period, time, id
    LIMIT {limit_rows};
    """

    # Connect to MySQL database
    conn = mysql.connector.connect(
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
        password=mysql_password,
        database=mysql_db,
    )
    # Use dictionary cursor for column-name-based access
    cur = conn.cursor(dictionary=True)

    # Initialize Kafka producer
    producer = Producer({
        "bootstrap.servers": kafka_bootstrap,
        "message.timeout.ms": 30000,
    })

    # Execute query and fetch all rows
    cur.execute(query)
    rows = cur.fetchall()

    print(f"Fetched {len(rows)} rows from MySQL ({mysql_db}.Shots).")
    print(f"Sending to Kafka topic '{topic}' on {kafka_bootstrap}...")

    sent = 0
    for r in rows:
        # Map database row to event schema
        event = {
            "event_id": r["id"],
            "game_id": r["game_id"],
            "player_id": r["player_id"],
            "result": int(r["result"]),
            "x": float(r["x"]),
            "y": float(r["y"]),
            "shot_type": r["shot_type"],
            "period": int(r["period"]),
            "time": str(r["time"]),
            "score_after_shot": r["score_after_shot"],
            "score_diff_before": int(r["score_differential_before_shot"]),
            "score_diff_after": int(r["score_differential_after_shot"]),
        }
        
        # Serialize event to JSON and send to Kafka
        producer.produce(topic, value=json.dumps(event, default=str), callback=delivery_report)
        sent += 1

        # Serve delivery callbacks and prevent local queue buildup
        if sent % 2000 == 0:
            producer.poll(0)

    # Ensure all messages are delivered before exiting
    producer.flush()
    
    # Clean up database resources
    cur.close()
    conn.close()
    print("Done")

if __name__ == "__main__":
    main()
