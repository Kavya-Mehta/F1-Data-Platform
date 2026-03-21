-- =============================================================================
-- F1 Data Platform: Raw Layer Schema (Bronze)
-- These tables receive data exactly as extracted from FastF1 API
-- No transformations, no cleaning — that's dbt's job
-- =============================================================================

-- Schema for raw data (Bronze layer)
CREATE SCHEMA IF NOT EXISTS raw;

-- Schema for dbt-transformed data (Silver + Gold layers)
CREATE SCHEMA IF NOT EXISTS analytics;

-- -------------------------------------------------------------------
-- raw.races — One row per race in the season
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.races (
    season          INTEGER NOT NULL,
    round_number    INTEGER NOT NULL,
    race_name       VARCHAR(200),
    circuit_name    VARCHAR(200),
    country         VARCHAR(100),
    race_date       DATE,
    loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (season, round_number)
);

-- -------------------------------------------------------------------
-- raw.results — One row per driver per race
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.results (
    season              INTEGER NOT NULL,
    round_number        INTEGER NOT NULL,
    driver_abbr         VARCHAR(10) NOT NULL,
    driver_full_name    VARCHAR(200),
    team_name           VARCHAR(200),
    grid_position       INTEGER,
    finish_position     FLOAT,
    classified_position VARCHAR(10),
    status              VARCHAR(100),
    points              FLOAT,
    is_dnf              BOOLEAN,
    loaded_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (season, round_number, driver_abbr)
);

-- -------------------------------------------------------------------
-- raw.lap_times — One row per driver per lap
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.lap_times (
    season          INTEGER NOT NULL,
    round_number    INTEGER NOT NULL,
    driver_abbr     VARCHAR(10) NOT NULL,
    lap_number      INTEGER NOT NULL,
    lap_time_ms     FLOAT,
    sector1_ms      FLOAT,
    sector2_ms      FLOAT,
    sector3_ms      FLOAT,
    compound        VARCHAR(20),
    tyre_life       INTEGER,
    is_personal_best BOOLEAN,
    loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (season, round_number, driver_abbr, lap_number)
);

-- -------------------------------------------------------------------
-- Indexes for common query patterns
-- -------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_results_driver ON raw.results(driver_abbr);
CREATE INDEX IF NOT EXISTS idx_results_team ON raw.results(team_name);
CREATE INDEX IF NOT EXISTS idx_laps_driver ON raw.lap_times(driver_abbr);
CREATE INDEX IF NOT EXISTS idx_laps_round ON raw.lap_times(round_number);