{{ config(tags=['monthly-full', 'nice-history']) }}

-- Run the season-boundary fixture with the monthly flu family.
-- depends_on: {{ ref('fct_person_flu_vaccination_nice_indicators_by_month') }}

WITH reference_dates AS (
    SELECT
        column1::DATE AS reporting_date,
        column2::NUMBER AS expected_season_year
    FROM VALUES
        ('2021-03-31', 2020),
        ('2021-04-01', 2020),
        ('2026-03-30', 2024),
        ('2026-03-31', 2025),
        ('2026-04-01', 2025),
        ('2026-12-31', 2025)
)
SELECT COUNT(*) AS failures
FROM reference_dates
WHERE {{ nice_flu_season_year('reporting_date') }} <> expected_season_year
HAVING COUNT(*) <> 0
