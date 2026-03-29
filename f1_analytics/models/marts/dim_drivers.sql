with results as (
    select * from {{ ref('stg_results') }}
),

features as (
    select * from {{ ref('int_driver_race_features') }}
),

latest_team as (
    select
        driver_abbr,
        team_name
    from results
    qualify row_number() over (partition by driver_abbr order by round_number desc) = 1
),

driver_stats as (
    select
        driver_abbr,
        round(avg(top3_rate)::numeric, 3) as career_top3_rate,
        round(avg(dnf_rate)::numeric, 3) as career_dnf_rate,
        round(avg(finish_consistency)::numeric, 3) as career_finish_consistency
    from features
    group by driver_abbr
)

select
    r.driver_abbr,
    r.driver_name,
    lt.team_name as current_team,
    ds.career_top3_rate,
    ds.career_dnf_rate,
    ds.career_finish_consistency
from (
    select driver_abbr, MIN(driver_name) as driver_name
    from results
    group by driver_abbr
) r
left join latest_team lt on r.driver_abbr = lt.driver_abbr
left join driver_stats ds on r.driver_abbr = ds.driver_abbr
order by r.driver_abbr

