with cumulative as (
    select * from {{ ref('int_driver_cumulative_stats') }}
),

laps as (
    select * from {{ ref('int_lap_analysis') }}
),

rolling_lap_stats as (
    select
        season,
        round_number,
        driver_abbr,
        avg(lap_time_std) over (
            partition by driver_abbr
            order by round_number
            rows between 6 preceding and 1 preceding
        ) as avg_lap_consistency,
        avg(relative_pace) over (
            partition by driver_abbr
            order by round_number
            rows between 6 preceding and 1 preceding
        ) as avg_relative_pace
    from laps
),

team_agg as (
    select
        season,
        round_number,
        team_name,
        1 as team_entry,
        case when boolor_agg(is_dnf) then 1 else 0 end as team_dnf_this_round
    from {{ ref('stg_results') }}
    group by season, round_number, team_name
),

team_reliability as (
    select
        season,
        round_number,
        team_name,
        case
            when sum(team_entry) over (
                partition by team_name
                order by round_number
                rows between unbounded preceding and 1 preceding
            ) > 0
            then 1.0 - (
                sum(team_dnf_this_round) over (
                    partition by team_name
                    order by round_number
                    rows between unbounded preceding and 1 preceding
                )::numeric /
                sum(team_entry) over (
                    partition by team_name
                    order by round_number
                    rows between unbounded preceding and 1 preceding
                )
            )
            else 0.9
        end as team_reliability
    from team_agg
),

grid_expectations as (
    select
        grid_position,
        avg(finish_position) as expected_finish
    from {{ ref('stg_results') }}
    where not is_dnf
    group by grid_position
),

perf_vs_expected as (
    select
        r.season,
        r.round_number,
        r.driver_abbr,
        avg(ge.expected_finish - r.finish_position) over (
            partition by r.driver_abbr
            order by r.round_number
            rows between 6 preceding and 1 preceding
        ) as avg_perf_vs_expected
    from {{ ref('stg_results') }} r
    left join grid_expectations ge
        on r.grid_position = ge.grid_position
    where not r.is_dnf
)

select
    c.season,
    c.round_number,
    c.driver_abbr,
    c.team_name,
    c.finish_position,
    c.grid_position,
    c.is_dnf,

    ln(c.grid_position::numeric + 1) as grid_log,
    c.top3_rate,
    c.top5_rate,
    c.top10_rate,
    c.top15_rate,
    c.finish_consistency_filled as finish_consistency,
    coalesce(pve.avg_perf_vs_expected, 0) as perf_vs_expected,
    coalesce(rls.avg_lap_consistency, 3.0) as lap_time_consistency,
    coalesce(rls.avg_relative_pace, 5.0) as relative_pace,
    c.dnf_rate,
    coalesce(tr.team_reliability, 0.9) as team_reliability,
    c.prev_cumulative_points as driver_points,
    c.prev_team_points as team_points,
    c.points_momentum

from cumulative c
left join rolling_lap_stats rls
    on c.season = rls.season
    and c.round_number = rls.round_number
    and c.driver_abbr = rls.driver_abbr
left join team_reliability tr
    on c.season = tr.season
    and c.round_number = tr.round_number
    and c.team_name = tr.team_name
left join perf_vs_expected pve
    on c.season = pve.season
    and c.round_number = pve.round_number
    and c.driver_abbr = pve.driver_abbr

