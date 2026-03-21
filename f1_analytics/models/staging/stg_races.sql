with source as (
    select * from {{ source('raw', 'races') }}
),

cleaned as (
    select
        season,
        round_number,
        trim(race_name) as race_name,
        trim(circuit_name) as circuit_name,
        trim(country) as country,
        race_date,
        loaded_at
    from source
    where season is not null
      and round_number is not null
)

select * from cleaned