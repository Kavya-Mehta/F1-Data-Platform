# F1 Data Platform

An end-to-end data engineering platform that ingests Formula 1 race data, transforms it through Medallion architecture layers using dbt, orchestrates pipelines with Airflow, streams real-time events through Kafka, and serves analytics dashboards via Power BI connected to Snowflake.

## Tech Stack

| Layer          | Technology             |
| -------------- | ---------------------- |
| Extraction     | Python, FastF1 API     |
| Storage        | PostgreSQL → Snowflake |
| Transformation | dbt Core               |
| Orchestration  | Apache Airflow         |
| Streaming      | Apache Kafka           |
| Data Quality   | Great Expectations     |
| Visualization  | Power BI               |
| Infrastructure | Docker, Docker Compose |

## Project Structure

```
f1-data-platform/
├── airflow/
│   ├── dags/
│   ├── logs/
│   └── plugins/
├── extract/
│   ├── load_f1_data.py
│   └── requirements.txt
├── f1_analytics/
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
                                      ↓
                     Great Expectations Data Quality Monitoring
                                      ↓
                     Kafka Streaming → Snowflake (Phase 3)
                                      ↓
                          Power BI Dashboards (Phase 3)
```

## Medallion Architecture

**Bronze (Raw):** Raw F1 race data loaded from FastF1 API into PostgreSQL — races, results, and lap times across the full 2025 season.

**Silver (Staging):** Cleaned and typed data with DNF flags, outlier lap removal using a 1.25x median filter, log-transformed grid positions, and points calculated via CASE WHEN logic.

**Gold (Marts):** Star schema dimensional model with 3 dimension tables and 4 fact tables served to Power BI dashboards.

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

## Airflow Orchestration

The entire pipeline is automated as an Airflow DAG running on a weekly schedule:

```
extract_f1_data → dbt_run → dbt_test → great_expectations_validation
```

- **extract_f1_data** — runs the FastF1 Python extraction script and loads raw data into PostgreSQL
- **dbt_run** — executes all dbt models through Bronze → Silver → Gold layers
- **dbt_test** — runs all 71 data quality tests across every layer
- **great_expectations_validation** — statistical data quality checks on row counts, null rates, and value distributions

Airflow UI available at `http://localhost:8080` after running `docker-compose up -d`.

## Data Quality

- 71 dbt tests passing across all layers
- Tests between every layer — not_null, unique, accepted_range
- Custom SQL tests for orphan laps and duplicate finish positions
- SCD Type 2 snapshots tracking driver-team changes (captured Lawson and Tsunoda mid-season transfers in 2025)
- Great Expectations suites monitoring raw layer statistics

## Dataset

| Table         | Rows   |
| ------------- | ------ |
| raw.races     | 24     |
| raw.results   | 479    |
| raw.lap_times | 26,692 |

2025 Championship Result: NOR 407pts, PIA 395pts, VER 394pts

## Setup

### Prerequisites

- Docker Desktop
- Python 3.10+
- dbt-postgres 1.7.13

### Run Locally

```bash
# Clone the repository
git clone https://github.com/Kavya-Mehta/F1-Data-Platform.git
cd F1-Data-Platform

# Create and activate virtual environment
python -m venv venv
venv\Scripts\activate

# Install Python dependencies
pip install -r extract/requirements.txt

# Start all services (PostgreSQL + Airflow)
docker-compose up -d

# Load 2025 F1 data
python extract/load_f1_data.py

# Run dbt transformations
cd f1_analytics
dbt deps
dbt run
dbt test

# Access Airflow UI
# Open http://localhost:8080
# Username: admin
# Password: admin123
```

## Phases

- **Phase 1 (Complete):** Batch ELT pipeline — FastF1 → PostgreSQL → dbt Medallion layers with star schema, 14 engineered features, SCD Type 2, 71 tests passing
- **Phase 2 (In Progress):** Airflow orchestration + Great Expectations data quality monitoring
- **Phase 3 (Planned):** Kafka streaming + Snowflake + Power BI dashboards
