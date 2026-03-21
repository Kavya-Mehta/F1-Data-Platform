with races as (
    select * from {{ ref('stg_races') }}
),

results as (
    select * from {{ ref('stg_results') }}
),

race_stats as (
    select
        r.round_number,
        count(case when res.is_dnf then 1 end) as dnf_count,
        count(distinct res.driver_abbr) as total_starters
    from races r
    left join results res
        on r.round_number = res.round_number
    group by r.round_number
)

select
    rc.circuit_name,
    rc.country,
    rc.race_name,
    rc.round_number as most_recent_round,
    rc.race_date,
    rs.total_starters,
    rs.dnf_count,
    round(rs.dnf_count::numeric / nullif(rs.total_starters, 0), 3) as circuit_dnf_rate
from races rc
left join race_stats rs
    on rc.round_number = rs.round_number
order by rc.round_number