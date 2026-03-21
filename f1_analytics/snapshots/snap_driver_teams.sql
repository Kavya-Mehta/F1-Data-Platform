{% snapshot snap_driver_teams %}

{{
    config(
        target_schema='snapshots',
        unique_key='driver_abbr',
        strategy='check',
        check_cols=['team_name'],
        invalidate_hard_deletes=True
    )
}}

SELECT
    driver_abbr,
    MIN(driver_name) AS driver_name,
    team_name,
    CURRENT_TIMESTAMP AS updated_at
FROM {{ ref('stg_results') }}
GROUP BY driver_abbr, team_name

{% endsnapshot %}