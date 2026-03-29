import json
import time
import uuid
import os
import psycopg2
from datetime import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

# PostgreSQL connection
conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=os.getenv("POSTGRES_PORT", 5433),
    dbname=os.getenv("POSTGRES_DB", "f1_warehouse"),
    user=os.getenv("POSTGRES_USER", "f1admin"),
    password=os.getenv("POSTGRES_PASSWORD", "f1analytics2025")
)

cursor = conn.cursor()

# Fetch lap times joined with race info
cursor.execute("""
    SELECT
        r.round_number AS race_id,
        r.race_name,
        lt.driver_abbr,
        lt.lap_number,
        ROUND((lt.lap_time_ms / 1000.0)::numeric, 3) AS lap_time_sec,
        ROUND((lt.sector1_ms / 1000.0)::numeric, 3)  AS sector1_sec,
        ROUND((lt.sector2_ms / 1000.0)::numeric, 3)  AS sector2_sec,
        ROUND((lt.sector3_ms / 1000.0)::numeric, 3)  AS sector3_sec,
        lt.is_personal_best
    FROM raw.lap_times lt
    JOIN raw.races r ON lt.season = r.season AND lt.round_number = r.round_number
    ORDER BY r.round_number, lt.lap_number, lt.driver_abbr
    LIMIT 500
""")

rows = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print(f"Sending {len(rows)} lap events to Kafka...")

for row in rows:
    event = dict(zip(columns, row))
    event["event_id"] = str(uuid.uuid4())
    event["event_timestamp"] = datetime.utcnow().isoformat()

    # Convert any non-serializable types
    for key, value in event.items():
        if hasattr(value, 'item'):
            event[key] = value.item()
        if hasattr(value, '__float__'):
            try:
                event[key] = float(value)
            except Exception:
                pass

    producer.send("f1_lap_events", value=event)
    print(f"Sent: {event['driver_abbr']} Lap {event['lap_number']} - {event['race_name']}")
    time.sleep(0.1)

producer.flush()
print("All events sent to Kafka!")

cursor.close()
conn.close()

