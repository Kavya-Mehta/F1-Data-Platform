-- This test checks that finish positions are valid whole numbers between 1 and 30.
-- It also checks that within each race, no two drivers share the same finish position.
-- The test passes if zero rows are returned.

WITH duplicate_positions AS (
    SELECT
        round_number,
        finish_position,
        COUNT(*) AS driver_count
    FROM {{ ref('stg_results') }}
    WHERE finish_position IS NOT NULL
    GROUP BY round_number, finish_position
    HAVING COUNT(*) > 1
)

SELECT
    r.round_number,
    r.driver_abbr,
    r.finish_position
FROM {{ ref('stg_results') }} r
WHERE
    r.finish_position < 1
    OR r.finish_position > 30
    OR EXISTS (
        SELECT 1 FROM duplicate_positions d
        WHERE d.round_number = r.round_number
        AND d.finish_position = r.finish_position
    )