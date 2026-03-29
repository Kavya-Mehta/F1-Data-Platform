import os
import psycopg2
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

# PostgreSQL connection
pg_conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=os.getenv("POSTGRES_PORT", 5433),
    dbname=os.getenv("POSTGRES_DB", "f1_warehouse"),
    user=os.getenv("POSTGRES_USER", "f1admin"),
    password=os.getenv("POSTGRES_PASSWORD", "f1analytics2025")
)

# Snowflake connection
sf_conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE")
)

pg_cursor = pg_conn.cursor()
sf_cursor = sf_conn.cursor()

print("Connected to both PostgreSQL and Snowflake!")

# Load races
print("Loading races...")
pg_cursor.execute("SELECT season, round_number, race_name, circuit_name, country, race_date FROM raw.races")
races = pg_cursor.fetchall()
sf_cursor.executemany("""
    INSERT INTO races (season, round_number, race_name, circuit_name, country, race_date)
    VALUES (%s, %s, %s, %s, %s, %s)
""", races)
sf_conn.commit()
print(f"Loaded {len(races)} races")

# Load results
print("Loading results...")
pg_cursor.execute("SELECT season, round_number, driver_abbr, driver_full_name, team_name, grid_position, finish_position, classified_position, status, points, is_dnf FROM raw.results")
results = pg_cursor.fetchall()
sf_cursor.executemany("""
    INSERT INTO results (season, round_number, driver_abbr, driver_full_name, team_name, grid_position, finish_position, classified_position, status, points, is_dnf)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
""", results)
sf_conn.commit()
print(f"Loaded {len(results)} results")

# Load lap times
print("Loading lap times (this may take a minute)...")
pg_cursor.execute("SELECT season, round_number, driver_abbr, lap_number, lap_time_ms, sector1_ms, sector2_ms, sector3_ms, compound, tyre_life, is_personal_best FROM raw.lap_times")
laps = pg_cursor.fetchall()

# Insert in batches of 1000
batch_size = 1000
for i in range(0, len(laps), batch_size):
    batch = laps[i:i+batch_size]
    sf_cursor.executemany("""
        INSERT INTO lap_times (season, round_number, driver_abbr, lap_number, lap_time_ms, sector1_ms, sector2_ms, sector3_ms, compound, tyre_life, is_personal_best)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, batch)
    sf_conn.commit()
    print(f"Loaded {min(i+batch_size, len(laps))}/{len(laps)} lap times")

print("All data loaded into Snowflake!")

pg_cursor.close()
pg_conn.close()
sf_cursor.close()
sf_conn.close()
