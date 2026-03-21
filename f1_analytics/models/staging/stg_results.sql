with source as (
    select * from {{ source('raw', 'results') }}
),

cleaned as (
    select
        season,
        round_number,
        driver_abbr,
        trim(driver_full_name) as driver_name,
        trim(team_name) as team_name,
        coalesce(grid_position, 20) as grid_position,
        ln(coalesce(grid_position, 20)::numeric + 1) as grid_log,
        finish_position,
        classified_position,
        status,
        case
            when finish_position = 1 then 25
            when finish_position = 2 then 18
            when finish_position = 3 then 15
            when finish_position = 4 then 12
            when finish_position = 5 then 10
            when finish_position = 6 then 8
            when finish_position = 7 then 6
            when finish_position = 8 then 4
            when finish_position = 9 then 2
            when finish_position = 10 then 1
            else 0
        end as points,
        coalesce(is_dnf, false) as is_dnf,
        case when points > 0 then true else false end as scored_points,
        case
            when is_dnf then 'DNF'
            when finish_position <= 3 then 'Podium'
            when finish_position <= 5 then 'Top 5'
            when finish_position <= 10 then 'Points'
            when finish_position <= 15 then 'Midfield'
            else 'Backmarker'
        end as finish_category,
        loaded_at
    from source
    where driver_abbr is not null
)

select * from cleaned