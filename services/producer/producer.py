import os
import json
from dotenv import load_dotenv
from confluent_kafka import Producer
import mysql.connector
from datetime import datetime, timezone

load_dotenv()

def env_int(key: str, default: int) -> int:
    v = os.getenv(key, str(default))
    return int(v)

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")

def main():
    # Kafka configuration
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC", "shots_raw")

    # MySQL configuration
    mysql_host = os.getenv("MYSQL_HOST", "127.0.0.1")
    mysql_port = env_int("MYSQL_PORT", 8889)
    mysql_user = os.getenv("MYSQL_USER", "root")
    mysql_password = os.getenv("MYSQL_PASSWORD", "")
    mysql_db = os.getenv("MYSQL_DATABASE", "basketball_optimalization")

    # Connect to MySQL
    conn = mysql.connector.connect(
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
        password=mysql_password,
        database=mysql_db,
    )
    cur = conn.cursor(dictionary=True)

    # Read last processed game_id
    cur.execute("SELECT last_processed_game_id FROM pipeline_state WHERE id = 1;")
    state = cur.fetchone()
    last_id = int(state["last_processed_game_id"]) if state else 0

    # Pick next game_id from Games
    cur.execute(
        """
        SELECT id AS game_id
        FROM Games
        WHERE id > %s
        ORDER BY id ASC
        LIMIT 1;
        """,
        (last_id,)
    )
    nxt = cur.fetchone()

    if not nxt:
        print(f"No new game found after game_id={last_id}. Nothing to stream.")
        cur.execute("INSERT INTO pipeline_runs (game_id, status) VALUES (%s, 'SKIPPED');", (None,))
        conn.commit()
        cur.close()
        conn.close()
        return

    game_id = int(nxt["game_id"])
    print(f"Next game to stream: game_id={game_id}")

    # Log run STARTED
    cur.execute("INSERT INTO pipeline_runs (game_id, status) VALUES (%s, 'STARTED');", (game_id,))
    run_id = cur.lastrowid
    conn.commit()

    # Load all shots for this game
    cur.execute(
        """
        SELECT
          id, game_id, player_id, result, x, y, shot_type, period, time,
          score_after_shot, score_differential_after_shot, score_differential_before_shot
        FROM Shots
        WHERE game_id = %s
        ORDER BY period, time, id;
        """,
        (game_id,)
    )
    shots = cur.fetchall()
    print(f"Fetched {len(shots)} shots for game_id={game_id}")

    # Initialize Kafka producer
    producer = Producer({
        "bootstrap.servers": kafka_bootstrap,
        "message.timeout.ms": 30000,
    })

    try:
        sent = 0
        for r in shots:
            # Map database row to event schema
            event = {
                "type": "SHOT",
                "event_id": r["id"],
                "run_id": run_id,
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
                "emitted_at": datetime.now(timezone.utc).isoformat(),
            }
            

            # key=run_id groups events per pipeline run
            producer.produce(
                topic,
                key=str(run_id),
                value=json.dumps(event, default=str),
                callback=delivery_report
            )
            sent += 1

            # Serve delivery callbacks and prevent local queue buildup
            if sent % 2000 == 0:
                producer.poll(0)

        end_event = {
            "type": "RUN_END",
            "run_id": run_id,
            "game_id": game_id,
            "shots_sent": len(shots),
            "emitted_at": datetime.now(timezone.utc).isoformat(),
        }

        producer.produce(
            topic,
            key=str(run_id),
            value=json.dumps(end_event, default=str),
            callback=delivery_report
        )

        # Ensure all messages are delivered before exiting
        producer.flush()

        # Update state only after successful send
        cur.execute("UPDATE pipeline_state SET last_processed_game_id = %s WHERE id = 1;", (game_id,))
        cur.execute("UPDATE pipeline_runs SET status='SUCCESS', finished_at=NOW() WHERE run_id=%s;", (run_id,))
        conn.commit()

        print(f"SUCCESS: streamed game_id={game_id} and updated pipeline_state.")

    except Exception as e:
        cur.execute(
            "UPDATE pipeline_runs SET status='ERROR', finished_at=NOW(), error_message=%s WHERE run_id=%s;",
            (str(e), run_id)
        )
        conn.commit()
        raise

    finally:
        # Clean up database resources
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()