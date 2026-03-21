with source as (
    select * from {{ source('raw', 'lap_times') }}
),

with_seconds as (
    select
        season,
        round_number,
        driver_abbr,
        lap_number,
        lap_time_ms / 1000.0 as lap_time_seconds,
        sector1_ms / 1000.0 as sector1_seconds,
        sector2_ms / 1000.0 as sector2_seconds,
        sector3_ms / 1000.0 as sector3_seconds,
        compound,
        tyre_life,
        is_personal_best,
        loaded_at
    from source
    where lap_time_ms is not null
      and lap_time_ms > 0
      and driver_abbr is not null
),

median_times as (
    select
        season,
        round_number,
        driver_abbr,
        percentile_cont(0.5) within group (order by lap_time_seconds) as median_lap_time
    from with_seconds
    group by season, round_number, driver_abbr
),

flagged as (
    select
        ws.*,
        mt.median_lap_time,
        case
            when ws.lap_time_seconds > mt.median_lap_time * 1.25 then true
            else false
        end as is_outlier
    from with_seconds ws
    inner join median_times mt
        on ws.season = mt.season
        and ws.round_number = mt.round_number
        and ws.driver_abbr = mt.driver_abbr
)

select * from flagged