-- Each breakdown must split the same people as its Total row: per indicator and
-- practice, the breakdown values sum to the Total denominator and numerator.
-- Returns indicator, practice and breakdown where they differ.

WITH sums AS (
    SELECT
        indicator_id,
        practice_code,
        breakdown,
        SUM(denominator) AS denominator,
        SUM(numerator) AS numerator
    FROM {{ ref('nice_indicator_explorer_achievement') }}
    GROUP BY indicator_id, practice_code, breakdown
)

SELECT
    b.indicator_id,
    b.practice_code,
    b.breakdown
FROM sums AS b
INNER JOIN sums AS t
    ON b.indicator_id = t.indicator_id
    AND b.practice_code = t.practice_code
    AND t.breakdown = 'Total'
WHERE b.breakdown <> 'Total'
    AND (b.denominator <> t.denominator OR b.numerator <> t.numerator)
