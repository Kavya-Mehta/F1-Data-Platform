with results as (
    select * from {{ ref('stg_results') }}
),

team_stats as (
    select
        team_name,
        count(*) as total_race_entries,
        count(case when finish_position = 1 and not is_dnf then 1 end) as total_wins,
        count(case when finish_position <= 3 and not is_dnf then 1 end) as total_podiums,
        count(case when is_dnf then 1 end) as total_dnfs,
        round(
            count(case when not is_dnf then 1 end)::numeric / count(*)::numeric, 3
        ) as reliability_rate,
        sum(points) as total_constructor_points
    from results
    group by team_name
)

select
    team_name,
    total_race_entries,
    total_wins,
    total_podiums,
    total_dnfs,
    reliability_rate,
    total_constructor_points
from team_stats
order by total_constructor_points desc