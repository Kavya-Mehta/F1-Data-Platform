with clean_laps as (
    select *
    from {{ ref('stg_lap_times') }}
    where is_outlier = false
),

driver_lap_stats as (
    select
        season,
        round_number,
        driver_abbr,
        count(*) as valid_laps,
        avg(lap_time_seconds) as mean_lap_time,
        stddev(lap_time_seconds) as lap_time_std
    from clean_laps
    group by season, round_number, driver_abbr
    having count(*) >= 5
),

leader_pace as (
    select
        d.season,
        d.round_number,
        d.mean_lap_time as leader_mean_lap_time
    from driver_lap_stats d
    inner join {{ ref('stg_results') }} r
        on d.season = r.season
        and d.round_number = r.round_number
        and d.driver_abbr = r.driver_abbr
    where r.finish_position = 1
)

select
    d.season,
    d.round_number,
    d.driver_abbr,
    d.valid_laps,
    d.mean_lap_time,
    d.lap_time_std,
    l.leader_mean_lap_time,
    d.mean_lap_time - l.leader_mean_lap_time as relative_pace

from driver_lap_stats d
left join leader_pace l
    on d.season = l.season
    and d.round_number = l.round_number