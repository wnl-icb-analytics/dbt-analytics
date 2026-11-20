{{ config(tags=['monthly-full', 'nice-history']) }}

-- Each breakdown must split the same people as its Total row: per month-end,
-- indicator and practice, the breakdown values sum to the Total denominator and numerator.
-- Returns month-end, indicator, practice and breakdown where they differ.

WITH sums AS (
    SELECT
        reporting_date,
        indicator_id,
        practice_code,
        breakdown,
        SUM(denominator) AS denominator,
        SUM(numerator) AS numerator
    FROM {{ ref('nice_indicator_explorer_achievement_by_month') }}
    GROUP BY reporting_date, indicator_id, practice_code, breakdown
)

SELECT
    b.reporting_date,
    b.indicator_id,
    b.practice_code,
    b.breakdown
FROM sums AS b
INNER JOIN sums AS t
    ON b.reporting_date = t.reporting_date
    AND b.indicator_id = t.indicator_id
    AND b.practice_code = t.practice_code
    AND t.breakdown = 'Total'
WHERE b.breakdown <> 'Total'
    AND (b.denominator <> t.denominator OR b.numerator <> t.numerator)
