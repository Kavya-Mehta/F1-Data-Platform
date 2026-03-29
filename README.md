# F1 Data Platform

An end-to-end data engineering platform that ingests Formula 1 race data, transforms it through Medallion architecture layers using dbt, orchestrates pipelines with Airflow, streams real-time events through Kafka, and serves analytics dashboards via Power BI connected to Snowflake.

## Tech Stack

| Layer          | Technology                            |
| -------------- | ------------------------------------- |
| Extraction     | Python, FastF1 API                    |
| Storage        | PostgreSQL → Snowflake                |
| Transformation | dbt Core 1.6.0 (Postgres + Snowflake) |
| Orchestration  | Apache Airflow 2.8.1                  |
| Streaming      | Apache Kafka + Zookeeper              |
| Data Quality   | Great Expectations                    |
| Visualization  | Power BI                              |
| Infrastructure | Docker, Docker Compose                |

## Project Structure

```
f1-data-platform/
├── airflow/
│   ├── dags/
│   │   └── f1_pipeline.py
│   ├── logs/
│   └── plugins/
├── extract/
│   ├── load_f1_data.py
│   ├── load_snowflake.py
│   └── requirements.txt
├── kafka/
│   ├── producer.py
│   └── consumer.py
├── f1_analytics/
│   ├── profiles.yml
│   ├── dbt_project.yml
│   ├── packages.yml
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   │       ├── dim_drivers.sql
│   │       ├── dim_teams.sql
│   │       ├── dim_circuits.sql
│   │       ├── fct_race_results.sql
│   │       ├── fct_championship_standings.sql
│   │       ├── fct_driver_performance.sql
│   │       └── fct_race_summary.sql
│   ├── snapshots/
│   └── tests/
├── gx/
│   ├── great_expectations.yml
│   ├── expectations/
│   ├── checkpoints/
│   └── run_checkpoint.py
├── Dockerfile
├── docker-compose.yaml
├── init_db.sql
└── .env
```

## Architecture

```
FastF1 API → Python Extraction → PostgreSQL (Bronze)
                                      ↓
                          dbt Staging Layer (Silver)
                          stg_races, stg_results, stg_lap_times
                                      ↓
                       dbt Intermediate Layer (Features)
                       int_driver_cumulative_stats
                       int_lap_analysis
                       int_driver_race_features (14 features)
                                      ↓
                    dbt Marts Layer — Star Schema (Gold)
                    dim_drivers, dim_teams, dim_circuits
                    fct_race_results, fct_championship_standings
                    fct_driver_performance, fct_race_summary
                                      ↓
                         Apache Airflow DAG Orchestration
                         extract → dbt run → dbt test
                         → dbt snapshot → great_expectations_validate
                                      ↓
                     Great Expectations Data Quality Monitoring
                     48/48 checks passing on raw layer
                                      ↓
                Kafka Producer → Kafka Topic (f1_lap_events)
                         → Kafka Consumer → Snowflake RAW
                                      ↓
                  dbt Snowflake target → Snowflake GOLD layer
                  13/13 models passing in Snowflake
                                      ↓
                          Power BI Dashboards (Phase 3)
```

## Medallion Architecture

**Bronze (Raw):** Raw F1 race data loaded from FastF1 API into PostgreSQL — races, results, and lap times across the full 2025 season. Same raw tables replicated into Snowflake for cloud transformation.

**Silver (Staging):** Cleaned and typed data with DNF flags, outlier lap removal using a 1.25x median filter, log-transformed grid positions, and points calculated via CASE WHEN logic.

**Gold (Marts):** Star schema dimensional model with 3 dimension tables and 4 fact tables. Runs on both PostgreSQL (via Airflow) and Snowflake (cloud target for Power BI).

## Star Schema

The Gold layer follows a canonical star schema design:

**Dimension Tables:**

- `dim_drivers` — one row per driver with career stats and current team
- `dim_teams` — one row per constructor with reliability and points stats
- `dim_circuits` — one row per circuit with metadata and DNF rate

**Fact Tables:**

- `fct_race_results` — main grain fact, one row per driver per race with all 14 engineered features
- `fct_championship_standings` — cumulative points and position per driver per round
- `fct_driver_performance` — season-level aggregate metrics per driver
- `fct_race_summary` — race-level summary with winner, DNFs, and pace metrics

## 14 Engineered Features

All features use leakage-preventing window functions — Race N uses only data from Races 1 to N-1 (`ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING`).

| Feature              | Description                                |
| -------------------- | ------------------------------------------ |
| Grid Log             | Log-transformed starting grid position     |
| Top 3 Rate           | Historical podium finish rate              |
| Top 5 Rate           | Historical top-5 finish rate               |
| Top 10 Rate          | Historical top-10 finish rate              |
| Top 15 Rate          | Historical top-15 finish rate              |
| Finish Consistency   | Standard deviation of finishing positions  |
| Perf vs Expected     | Actual finish vs grid position expectation |
| Lap Time Consistency | Standard deviation of clean lap times      |
| Relative Pace        | Gap to race leader in seconds              |
| DNF Rate             | Historical mechanical failure rate         |
| Team Reliability     | Team-level mechanical reliability rate     |
| Driver Points        | Cumulative championship points             |
| Team Points          | Cumulative constructor points              |
| Points Momentum      | Rolling 6-race average points              |

## Kafka Streaming

A simulated real-time streaming layer runs alongside the batch pipeline:

- **Producer** (`kafka/producer.py`) — reads lap times from PostgreSQL and streams them one event at a time to the `f1_lap_events` Kafka topic, simulating live F1 telemetry
- **Consumer** (`kafka/consumer.py`) — reads from the Kafka topic and writes each event into Snowflake's `RAW.kafka_lap_events` table in real time
- **Topic:** `f1_lap_events`
- **Events streamed:** 500 lap events per run (configurable)
- **Data verified:** 1,159+ rows confirmed in Snowflake after streaming

This demonstrates a hybrid architecture — batch ELT via Airflow/dbt running in parallel with real-time streaming via Kafka.

## Airflow Orchestration

The entire batch pipeline is automated as an Airflow DAG running on a weekly schedule:

```
extract_f1_data → dbt_run → dbt_test → dbt_snapshot → great_expectations_validate
```

- **extract_f1_data** — runs the FastF1 Python extraction script and loads raw data into PostgreSQL
- **dbt_run** — executes all 13 dbt models through Bronze → Silver → Gold layers
- **dbt_test** — runs all 71 data quality tests across every layer
- **dbt_snapshot** — runs SCD Type 2 snapshot to track mid-season driver-team transfers
- **great_expectations_validate** — runs 48 statistical data quality checks on row counts, null rates, and value distributions

Airflow UI available at `http://localhost:8080` after running `docker-compose up -d`.

## Data Quality

- 71 dbt tests passing across all layers
- Tests between every layer — not_null, unique, accepted_range
- Custom SQL tests for orphan laps and duplicate finish positions
- SCD Type 2 snapshot tracking driver-team changes (captured Lawson and Tsunoda mid-season transfers in 2025)
- 48/48 Great Expectations checks passing on the raw results layer

## Dataset

| Table         | Rows   |
| ------------- | ------ |
| raw.races     | 24     |
| raw.results   | 479    |
| raw.lap_times | 26,692 |

2025 Championship Standings: NOR 407pts (P1), PIA 395pts (P2), VER 394pts (P3)

## Setup

### Prerequisites

- Docker Desktop (with WSL 2 on Windows)
- Python 3.10+
- Snowflake account (free trial works)
- Power BI Desktop

### 1. Clone the Repository

```bash
git clone https://github.com/Kavya-Mehta/F1-Data-Platform.git
cd F1-Data-Platform
```

### 2. Create Virtual Environment

```bash
python -m venv venv
venv\Scripts\activate        # Windows
source venv/bin/activate     # Mac/Linux
```

### 3. Install Python Dependencies

```bash
pip install -r extract/requirements.txt
pip install kafka-python snowflake-connector-python dbt-snowflake==1.6.0
```

### 4. Configure Environment Variables

Create a `.env` file in the root directory with the following values:

```
# PostgreSQL
POSTGRES_USER=your_postgres_user
POSTGRES_PASSWORD=your_postgres_password
POSTGRES_DB=your_database_name
POSTGRES_PORT=5433

# Airflow
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=your_airflow_password
AIRFLOW_ADMIN_EMAIL=your@email.com
AIRFLOW__CORE__FERNET_KEY=your_fernet_key

# Snowflake
SNOWFLAKE_ACCOUNT=your-account-identifier
SNOWFLAKE_USER=your-snowflake-username
SNOWFLAKE_PASSWORD=your-snowflake-password
SNOWFLAKE_DATABASE=F1_DW
SNOWFLAKE_SCHEMA=RAW
SNOWFLAKE_WAREHOUSE=F1_WH
```

To find your Snowflake account identifier: log into Snowflake → click your name bottom left → Admin → Account → copy the Account Identifier (format: `ORGNAME-ACCOUNTNAME`).

### 5. Set Up Snowflake

In a Snowflake worksheet, run:

```sql
CREATE DATABASE IF NOT EXISTS F1_DW;
USE DATABASE F1_DW;
CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS GOLD;
CREATE WAREHOUSE IF NOT EXISTS F1_WH
  WITH WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;
```

### 6. Start Docker Services

```bash
# Build custom image and start PostgreSQL + Airflow
docker-compose build --no-cache
docker-compose up -d

# Start Kafka and Zookeeper
docker-compose up -d zookeeper kafka

# Verify all 5 containers are running
docker ps
```

### 7. Load Data

```bash
# Load 2025 F1 data into PostgreSQL
python extract/load_f1_data.py

# Load same data into Snowflake
python extract/load_snowflake.py
```

### 8. Run dbt

```bash
cd f1_analytics

# Run against PostgreSQL (used by Airflow)
dbt run --profiles-dir .

# Run against Snowflake (Gold layer for Power BI)
dbt run --target snowflake --profiles-dir .
```

### 9. Run Kafka Streaming

Open two terminals:

```bash
# Terminal 1 — start consumer first
python kafka/consumer.py

# Terminal 2 — run producer
python kafka/producer.py
```

### 10. Access Services

| Service    | URL / Location        | Credentials             |
| ---------- | --------------------- | ----------------------- |
| Airflow UI | http://localhost:8080 | admin/admin123          |
| PostgreSQL | localhost:5433        | f1admin/f1analytics2025 |
| Snowflake  | app.snowflake.com     | your credentials        |
| Kafka      | localhost:9092        | no auth required        |

## Key Engineering Decisions

- **Custom Docker image** — built on `apache/airflow:2.8.1-python3.10` to support fastf1==3.4.4 (requires Python 3.10+)
- **dbt dual targets** — `dev` target runs against PostgreSQL for Airflow batch pipeline; `snowflake` target runs against Snowflake for cloud Gold layer
- **Snowflake SQL compatibility** — replaced PostgreSQL-specific syntax (`DISTINCT ON`, `BOOL_OR`, correlated subqueries with `LIMIT`) with Snowflake equivalents (`QUALIFY ROW_NUMBER()`, `BOOLOR_AGG`)
- **Docker networking** — all services reference PostgreSQL by service name `postgres:5432` internally; port 5433 is only used for local host access
- **Leakage-preventing features** — all 14 engineered features use `ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING` to prevent data leakage
- **SCD Type 2** — snap_driver_teams snapshot captures real mid-season team changes
- **Kafka deduplication** — each event has a unique `event_id` (UUID) for downstream deduplication

## Phases

- **Phase 1 (Complete ✓):** Batch ELT pipeline — FastF1 → PostgreSQL → dbt Medallion layers with star schema, 14 engineered features, SCD Type 2, 71 tests passing, GitHub Actions CI
- **Phase 2 (Complete ✓):** Airflow orchestration with 5-task DAG, Great Expectations data quality monitoring (48/48 checks), fully containerized with custom Docker image
- **Phase 3 (Complete ✓):** Kafka streaming pipeline writing lap events to Snowflake, dbt Snowflake target with 13/13 models passing, Power BI dashboards connected to Snowflake Gold layer
