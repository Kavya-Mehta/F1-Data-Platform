with results as (
    select * from {{ ref('stg_results') }}
),

races as (
    select * from {{ ref('stg_races') }}
),

features as (
    select * from {{ ref('int_driver_race_features') }}
),

lap_analysis as (
    select * from {{ ref('int_lap_analysis') }}
)

select
    -- keys
    r.season,
    r.round_number,
    r.driver_abbr,
    r.team_name,

    -- race info
    rc.race_name,
    rc.circuit_name,
    rc.country,
    rc.race_date,

    -- result
    r.finish_position,
    r.grid_position,
    r.points,
    r.is_dnf,
    r.finish_category,

    -- lap metrics
    l.mean_lap_time,
    l.lap_time_std,
    l.relative_pace,

    -- engineered features (all leakage-free)
    f.grid_log,
    f.top3_rate,
    f.top5_rate,
    f.top10_rate,
    f.top15_rate,
    f.finish_consistency,
    f.perf_vs_expected,
    f.lap_time_consistency,
    f.dnf_rate,
    f.team_reliability,
    f.driver_points,
    f.team_points,
    f.points_momentum

from results r
left join races rc
    on r.season = rc.season
    and r.round_number = rc.round_number
left join features f
    on r.season = f.season
    and r.round_number = f.round_number
    and r.driver_abbr = f.driver_abbr
left join lap_analysis l
    on r.season = l.season
    and r.round_number = l.round_number
    and r.driver_abbr = l.driver_abbr