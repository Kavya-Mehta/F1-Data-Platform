import os
import sys
import fastf1
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

SEASON = int(os.getenv("F1_SEASON", 2025))
CACHE_DIR = os.getenv("F1_CACHE_DIR", "./f1_cache")

DB_CONFIG = {
    "host": "localhost",
    "port": os.getenv("POSTGRES_PORT", 5432),
    "dbname": os.getenv("POSTGRES_DB", "f1_warehouse"),
    "user": os.getenv("POSTGRES_USER", "f1admin"),
    "password": os.getenv("POSTGRES_PASSWORD", "f1analytics2025"),
}


def setup_cache():
    os.makedirs(CACHE_DIR, exist_ok=True)
    fastf1.Cache.enable_cache(CACHE_DIR)
    print(f"[OK] Cache enabled at: {CACHE_DIR}")


def get_db_connection():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    return conn


def get_total_rounds(season):
    schedule = fastf1.get_event_schedule(season)
    races = schedule[schedule['EventFormat'] != 'testing']
    return len(races)


def extract_race_data(season, round_number):
    try:
        print(f"  Loading Round {round_number}...", end=" ")
        session = fastf1.get_session(season, round_number, 'R')
        session.load(telemetry=False, laps=True, weather=False)

        event = session.event
        race_info = {
            "season": season,
            "round_number": round_number,
            "race_name": event["EventName"],
            "circuit_name": event.get("Location", "Unknown"),
            "country": event.get("Country", "Unknown"),
            "race_date": session.date.date() if hasattr(session, 'date') else None,
        }

        results = session.results.copy().reset_index(drop=True)
        laps = session.laps

        # --- Derive grid positions from Lap 1 position data ---
        grid_map = {}
        if laps is not None and not laps.empty:
            first_laps = laps[laps['LapNumber'] == 1][['Driver', 'Position']].dropna()
            for _, row in first_laps.iterrows():
                grid_map[row['Driver']] = int(row['Position'])

        # --- Derive finish positions from results row order ---
        # FastF1 returns results sorted by finishing order
        results_data = []
        for idx, row in results.iterrows():
            driver = row["Abbreviation"]
            finish_pos = idx + 1  # Row order = finishing order

            # Grid position: from lap 1 data, fallback to finish position
            grid_pos = grid_map.get(driver, finish_pos)

            # DNF detection from Status column
            status = row.get("Status", "Finished")
            is_dnf = False
            if pd.notna(status):
                status_str = str(status).lower()
                is_dnf = any(word in status_str for word in [
                    'retired', 'accident', 'collision', 'mechanical',
                    'engine', 'gearbox', 'hydraulic', 'electrical',
                    'spin', 'damage', 'puncture', 'brake', 'withdrew',
                    'disqualified', 'dns', 'dnf', 'not classified'
                ])

            # Also check: if driver completed way fewer laps than leader, likely DNF
            if not is_dnf and laps is not None and not laps.empty:
                driver_laps = laps[laps['Driver'] == driver]
                max_laps = laps['LapNumber'].max()
                if len(driver_laps) > 0 and driver_laps['LapNumber'].max() < max_laps * 0.9:
                    is_dnf = True

            # Points from official column, fallback to 0
            points = float(row.get("Points", 0)) if pd.notna(row.get("Points")) else 0.0

            results_data.append({
                "season": season,
                "round_number": round_number,
                "driver_abbr": driver,
                "driver_full_name": f"{row.get('FirstName', '')} {row.get('LastName', '')}".strip(),
                "team_name": row.get("TeamName", "Unknown"),
                "grid_position": grid_pos,
                "finish_position": float(finish_pos),
                "classified_position": str(finish_pos) if not is_dnf else "R",
                "status": str(status) if pd.notna(status) else "Unknown",
                "points": points,
                "is_dnf": is_dnf,
            })

        # --- Lap times ---
        lap_times_data = []
        try:
            if laps is not None and not laps.empty:
                for _, lap in laps.iterrows():
                    lap_time = lap.get("LapTime")
                    lap_time_ms = lap_time.total_seconds() * 1000 if pd.notna(lap_time) else None

                    s1 = lap.get("Sector1Time")
                    s2 = lap.get("Sector2Time")
                    s3 = lap.get("Sector3Time")

                    lap_times_data.append({
                        "season": season,
                        "round_number": round_number,
                        "driver_abbr": lap.get("Driver", "UNK"),
                        "lap_number": int(lap.get("LapNumber", 0)),
                        "lap_time_ms": lap_time_ms,
                        "sector1_ms": s1.total_seconds() * 1000 if pd.notna(s1) else None,
                        "sector2_ms": s2.total_seconds() * 1000 if pd.notna(s2) else None,
                        "sector3_ms": s3.total_seconds() * 1000 if pd.notna(s3) else None,
                        "compound": lap.get("Compound", None),
                        "tyre_life": int(lap.get("TyreLife", 0)) if pd.notna(lap.get("TyreLife")) else None,
                        "is_personal_best": bool(lap.get("IsPersonalBest", False)),
                    })
        except Exception as e:
            print(f"[WARN] Lap data issue: {e}")

        print(f"[OK] {len(results_data)} drivers, {len(lap_times_data)} laps")

        return {
            "race_info": race_info,
            "results": results_data,
            "lap_times": lap_times_data,
        }

    except Exception as e:
        print(f"[FAIL] {e}")
        return None



def load_to_postgres(conn, race_data):
    cursor = conn.cursor()

    try:
        ri = race_data["race_info"]
        cursor.execute("""
            INSERT INTO raw.races (season, round_number, race_name, circuit_name, country, race_date)
            VALUES (%(season)s, %(round_number)s, %(race_name)s, %(circuit_name)s, %(country)s, %(race_date)s)
            ON CONFLICT (season, round_number) DO UPDATE SET
                race_name = EXCLUDED.race_name,
                circuit_name = EXCLUDED.circuit_name,
                country = EXCLUDED.country,
                race_date = EXCLUDED.race_date,
                loaded_at = CURRENT_TIMESTAMP
        """, ri)

        for r in race_data["results"]:
            cursor.execute("""
                INSERT INTO raw.results
                    (season, round_number, driver_abbr, driver_full_name, team_name,
                     grid_position, finish_position, classified_position, status, points, is_dnf)
                VALUES
                    (%(season)s, %(round_number)s, %(driver_abbr)s, %(driver_full_name)s,
                     %(team_name)s, %(grid_position)s, %(finish_position)s,
                     %(classified_position)s, %(status)s, %(points)s, %(is_dnf)s)
                ON CONFLICT (season, round_number, driver_abbr) DO UPDATE SET
                    team_name = EXCLUDED.team_name,
                    grid_position = EXCLUDED.grid_position,
                    finish_position = EXCLUDED.finish_position,
                    classified_position = EXCLUDED.classified_position,
                    status = EXCLUDED.status,
                    points = EXCLUDED.points,
                    is_dnf = EXCLUDED.is_dnf,
                    loaded_at = CURRENT_TIMESTAMP
            """, r)

        if race_data["lap_times"]:
            cursor.execute(
                "DELETE FROM raw.lap_times WHERE season = %s AND round_number = %s",
                (ri["season"], ri["round_number"])
            )

            insert_sql = """
                INSERT INTO raw.lap_times
                    (season, round_number, driver_abbr, lap_number, lap_time_ms,
                     sector1_ms, sector2_ms, sector3_ms, compound, tyre_life, is_personal_best)
                VALUES %s
            """
            values = [
                (l["season"], l["round_number"], l["driver_abbr"], l["lap_number"],
                 l["lap_time_ms"], l["sector1_ms"], l["sector2_ms"], l["sector3_ms"],
                 l["compound"], l["tyre_life"], l["is_personal_best"])
                for l in race_data["lap_times"]
            ]
            execute_values(cursor, insert_sql, values, page_size=500)

        conn.commit()

    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()


def main():
    print("=" * 60)
    print(f"F1 Data Platform — Extract & Load")
    print(f"Season: {SEASON}")
    print("=" * 60)

    setup_cache()
    conn = get_db_connection()
    print(f"[OK] Connected to PostgreSQL: {DB_CONFIG['dbname']}")

    total_rounds = get_total_rounds(SEASON)
    print(f"[OK] Found {total_rounds} rounds in {SEASON} season\n")

    loaded = 0
    failed = 0

    for round_num in range(1, total_rounds + 1):
        race_data = extract_race_data(SEASON, round_num)

        if race_data:
            try:
                load_to_postgres(conn, race_data)
                loaded += 1
            except Exception as e:
                print(f"  [LOAD FAIL] Round {round_num}: {e}")
                failed += 1
        else:
            failed += 1

    conn.close()
    print(f"\n{'=' * 60}")
    print(f"Extraction Complete!")
    print(f"  Loaded:  {loaded} races")
    print(f"  Failed:  {failed} races")
    print(f"  Total:   {total_rounds} races")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
