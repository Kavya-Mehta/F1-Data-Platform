with results as (
    select * from {{ ref('stg_results') }}
),

races as (
    select * from {{ ref('stg_races') }}
),

laps as (
    select * from {{ ref('int_lap_analysis') }}
),

race_stats as (
    select
        season,
        round_number,
        count(distinct driver_abbr) as total_starters,
        count(case when is_dnf then 1 end) as total_dnfs,
        round(avg(case when not is_dnf then finish_position end)::numeric, 2) as avg_finish_position
    from results
    group by season, round_number
),

lap_stats as (
    select
        season,
        round_number,
        round(min(mean_lap_time)::numeric, 3) as fastest_avg_pace,
        round(avg(lap_time_std)::numeric, 3) as avg_lap_consistency
    from laps
    group by season, round_number
)

select
    rc.season,
    rc.round_number,
    rc.race_name,
    rc.circuit_name,
    rc.country,
    rc.race_date,

    (select r.driver_abbr from results r
     where r.round_number = rc.round_number and r.finish_position = 1
     limit 1) as winner,

    (select r.team_name from results r
     where r.round_number = rc.round_number and r.finish_position = 1
     limit 1) as winning_team,

    rs.total_starters,
    rs.total_dnfs,
    rs.avg_finish_position,
    ls.fastest_avg_pace,
    ls.avg_lap_consistency

from races rc
left join race_stats rs
    on rc.season = rs.season and rc.round_number = rs.round_number
left join lap_stats ls
    on rc.season = ls.season and rc.round_number = ls.round_number
