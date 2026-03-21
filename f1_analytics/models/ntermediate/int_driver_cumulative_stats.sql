with results as (
    select * from {{ ref('stg_results') }}
),

cumulative as (
    select
        season,
        round_number,
        driver_abbr,
        team_name,
        finish_position,
        grid_position,
        points,
        is_dnf,

        count(*) over (
            partition by driver_abbr
            order by round_number
            rows between unbounded preceding and 1 preceding
        ) as prev_races_started,

        count(case when finish_position <= 3 and not is_dnf then 1 end) over (
            partition by driver_abbr
            order by round_number
            rows between unbounded preceding and 1 preceding
        ) as prev_top3_count,

        count(case when finish_position <= 5 and not is_dnf then 1 end) over (
            partition by driver_abbr
            order by round_number
            rows between unbounded preceding and 1 preceding
        ) as prev_top5_count,

        count(case when finish_position <= 10 and not is_dnf then 1 end) over (
            partition by driver_abbr
            order by round_number
            rows between unbounded preceding and 1 preceding
        ) as prev_top10_count,

        count(case when finish_position <= 15 and not is_dnf then 1 end) over (
            partition by driver_abbr
            order by round_number
            rows between unbounded preceding and 1 preceding
        ) as prev_top15_count,

        count(case when is_dnf then 1 end) over (
            partition by driver_abbr
            order by round_number
            rows between unbounded preceding and 1 preceding
        ) as prev_dnf_count,

        coalesce(sum(points) over (
            partition by driver_abbr
            order by round_number
            rows between unbounded preceding and 1 preceding
        ), 0) as prev_cumulative_points,

        coalesce(sum(points) over (
            partition by team_name
            order by round_number
            rows between unbounded preceding and 1 preceding
        ), 0) as prev_team_points,

        coalesce(avg(points) over (
            partition by driver_abbr
            order by round_number
            rows between 5 preceding and 1 preceding
        ), 0) as points_momentum,

        stddev(case when not is_dnf then finish_position end) over (
            partition by driver_abbr
            order by round_number
            rows between 6 preceding and 1 preceding
        ) as finish_consistency

    from results
)

select
    *,

    case when prev_races_started > 0
        then prev_top3_count::numeric / prev_races_started
        else 0 end as top3_rate,

    case when prev_races_started > 0
        then prev_top5_count::numeric / prev_races_started
        else 0 end as top5_rate,

    case when prev_races_started > 0
        then prev_top10_count::numeric / prev_races_started
        else 0 end as top10_rate,

    case when prev_races_started > 0
        then prev_top15_count::numeric / prev_races_started
        else 0 end as top15_rate,

    case when prev_races_started > 0
        then prev_dnf_count::numeric / prev_races_started
        else 0 end as dnf_rate,

    coalesce(finish_consistency, 5.0) as finish_consistency_filled

from cumulative