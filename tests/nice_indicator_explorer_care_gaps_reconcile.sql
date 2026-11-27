{{ config(tags=['reconciliation']) }}

-- Care gaps must hold the same indicator results as the achievement Total rows: per practice,
-- results due equal the summed denominators and unmet results equal the summed not achieved.
-- Catches joins that multiply people before aggregation. Returns practices that differ.

WITH gaps AS (
    SELECT
        practice_code,
        SUM(indicators_due) AS indicators_due,
        SUM(unmet_results) AS unmet_results
    FROM {{ ref('nice_indicator_explorer_care_gaps') }}
    GROUP BY practice_code
),

achievement AS (
    SELECT
        practice_code,
        SUM(denominator) AS denominator,
        SUM(not_achieved) AS not_achieved
    FROM {{ ref('nice_indicator_explorer_achievement') }}
    WHERE breakdown = 'Total'
    GROUP BY practice_code
)

SELECT
    COALESCE(g.practice_code, a.practice_code) AS practice_code
FROM gaps AS g
FULL OUTER JOIN achievement AS a
    ON g.practice_code = a.practice_code
WHERE NOT EQUAL_NULL(g.indicators_due, a.denominator)
    OR NOT EQUAL_NULL(g.unmet_results, a.not_achieved)
