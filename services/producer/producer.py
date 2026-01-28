import os
import json
from dotenv import load_dotenv
from confluent_kafka import Producer
import mysql.connector

load_dotenv()

def env_int(key: str, default: int) -> int:
    v = os.getenv(key, str(default))
    return int(v)

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    # else:
    #     print(f"Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

def main():
    # --- Config from env ---
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC", "shots_raw")

    mysql_host = os.getenv("MYSQL_HOST", "127.0.0.1")
    mysql_port = env_int("MYSQL_PORT", 8889)
    mysql_user = os.getenv("MYSQL_USER", "root")
    mysql_password = os.getenv("MYSQL_PASSWORD", "")
    mysql_db = os.getenv("MYSQL_DATABASE", "basketball_optimalization")

    limit_rows = env_int("LIMIT_ROWS", 100)

    query = f"""
    SELECT
      id, game_id, player_id, result, x, y, shot_type, period, time,
      score_after_shot, score_differential_after_shot, score_differential_before_shot
    FROM Shots
    ORDER BY game_id, period, time, id
    LIMIT {limit_rows};
    """

    # --- Connect MySQL ---
    conn = mysql.connector.connect(
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
        password=mysql_password,
        database=mysql_db,
    )
    cur = conn.cursor(dictionary=True)

    # --- Connect Kafka (confluent-kafka) ---
    producer = Producer({
        "bootstrap.servers": kafka_bootstrap,
        # helps for local dev if broker hiccups
        "message.timeout.ms": 30000,
    })

    cur.execute(query)
    rows = cur.fetchall()

    print(f"Fetched {len(rows)} rows from MySQL ({mysql_db}.Shots).")
    print(f"Sending to Kafka topic '{topic}' on {kafka_bootstrap}...")

    sent = 0
    for r in rows:
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

        producer.produce(topic, value=json.dumps(event, default=str), callback=delivery_report)
        sent += 1

        # let producer handle delivery queue
        if sent % 2000 == 0:
            producer.poll(0)

    producer.flush()
    cur.close()
    conn.close()
    print("Done")

if __name__ == "__main__":
    main()
