with results as (
    select * from {{ ref('stg_results') }}
),

with_totals as (
    select
        season,
        round_number,
        driver_abbr,
        team_name,
        points as race_points,

        sum(points) over (
            partition by season, driver_abbr
            order by round_number
        ) as total_points

    from results
),

with_rankings as (
    select
        *,
        rank() over (
            partition by season, round_number
            order by total_points desc
        ) as championship_position
    from with_totals
)

select * from with_rankings
