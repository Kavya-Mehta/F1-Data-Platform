import json
import os
import snowflake.connector
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

# Snowflake connection
snowflake_conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE")
)

snowflake_cursor = snowflake_conn.cursor()
print("Connected to Snowflake successfully!")

# Kafka consumer
consumer = KafkaConsumer(
    "f1_lap_events",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id="f1_consumer_group"
)

print("Listening for Kafka events...")

for message in consumer:
    event = message.value

    snowflake_cursor.execute("""
        INSERT INTO kafka_lap_events (
            event_id,
            race_id,
            race_name,
            driver_code,
            lap_number,
            lap_time_sec,
            sector1_sec,
            sector2_sec,
            sector3_sec,
            is_personal_best,
            event_timestamp
        ) VALUES (
            %(event_id)s,
            %(race_id)s,
            %(race_name)s,
            %(driver_abbr)s,
            %(lap_number)s,
            %(lap_time_sec)s,
            %(sector1_sec)s,
            %(sector2_sec)s,
            %(sector3_sec)s,
            %(is_personal_best)s,
            %(event_timestamp)s
        )
    """, event)

    print(f"Inserted into Snowflake: {event['driver_abbr']} Lap {event['lap_number']} - {event['race_name']}")
    snowflake_conn.commit()