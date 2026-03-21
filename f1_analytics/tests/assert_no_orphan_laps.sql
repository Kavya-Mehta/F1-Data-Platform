-- This test checks that every lap record has a matching result record.
-- An "orphan lap" means we have lap data for a driver in a race
-- but no corresponding entry in results — which would indicate a data loading bug.
-- The test passes if zero rows are returned.

SELECT
    l.round_number,
    l.driver_abbr
FROM {{ ref('stg_lap_times') }} l
LEFT JOIN {{ ref('stg_results') }} r
    ON l.round_number = r.round_number
    AND l.driver_abbr = r.driver_abbr
WHERE r.driver_abbr IS NULL