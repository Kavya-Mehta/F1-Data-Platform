with features as (
    select * from {{ ref('int_driver_race_features') }}
),

results as (
    select * from {{ ref('stg_results') }}
)

select
    f.driver_abbr,
    f.team_name,

    count(*) as races_entered,
    count(case when not f.is_dnf then 1 end) as races_finished,
    count(case when f.finish_position <= 3 and not f.is_dnf then 1 end) as podiums,
    count(case when f.finish_position = 1 and not f.is_dnf then 1 end) as wins,
    sum(r.points) as total_points,

    round(avg(case when not f.is_dnf then f.finish_position end)::numeric, 2) as avg_finish,
    round(avg(f.grid_position)::numeric, 2) as avg_grid,
    round(avg(f.perf_vs_expected)::numeric, 2) as avg_perf_vs_expected,
    round(avg(f.lap_time_consistency)::numeric, 2) as avg_lap_consistency,
    round(avg(f.relative_pace)::numeric, 2) as avg_relative_pace,

    round(stddev(case when not f.is_dnf then f.finish_position end)::numeric, 2) as finish_stddev,

    round(max(f.top3_rate)::numeric, 3) as final_top3_rate,
    round(max(f.dnf_rate)::numeric, 3) as final_dnf_rate

from features f
inner join results r
    on f.season = r.season
    and f.round_number = r.round_number
    and f.driver_abbr = r.driver_abbr
group by f.driver_abbr, f.team_name