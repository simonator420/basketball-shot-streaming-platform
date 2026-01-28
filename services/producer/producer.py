import os
import json
from dotenv import load_dotenv
from kafka import KafkaProducer
import mysql.connector

load_dotenv()  # loads .env if present

def env_int(key: str, default: int) -> int:
    v = os.getenv(key, str(default))
    return int(v)

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

    # --- Connect Kafka ---
    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )

    # --- Fetch + send ---
    cur.execute(query)
    rows = cur.fetchall()
    print(f"Fetched {len(rows)} rows from MySQL ({mysql_db}.Shots).")
    print(f"Sending to Kafka topic '{topic}' on {kafka_bootstrap}...")

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
        producer.send(topic, event)

    producer.flush()
    producer.close()
    cur.close()
    conn.close()

    print("Done")

if __name__ == "__main__":
    main()
