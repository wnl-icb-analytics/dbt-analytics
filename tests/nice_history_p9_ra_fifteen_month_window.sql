{{ config(tags=['monthly-full', 'nice-history']) }}

{% set calculation = nice_ind110('by_month') %}
{% set calculation = calculation | replace(nice_reference_population('by_month') | string, 'SELECT * FROM synthetic_population') %}
{% set calculation = calculation | replace(ref('int_nice_ltc_population_by_month') | string, 'synthetic_profile') %}
{% set calculation = calculation | replace(ref('int_nice_review_evidence_by_month') | string, 'synthetic_reviews') %}

WITH synthetic_population AS (
    SELECT
        -9906::NUMBER AS person_id,
        column1::DATE AS reporting_date,
        40 AS age,
        'SYNTHETIC' AS practice_code,
        'Synthetic practice' AS practice_name
    FROM VALUES ('2024-09-30'), ('2024-10-31')
),
synthetic_profile AS (
    SELECT person_id, reporting_date, TRUE AS has_rheumatoid_arthritis
    FROM synthetic_population
),
synthetic_reviews AS (
    SELECT person_id, reporting_date,
        '2023-06-30'::DATE AS latest_rheumatoid_arthritis_review_date
    FROM synthetic_population
),
actual AS ({{ calculation }})
SELECT COUNT(*) AS rows_total
FROM actual
HAVING COUNT(*) <> 2
    OR COUNT_IF(reporting_date = '2024-09-30' AND is_in_numerator
        AND latest_record_date = '2023-06-30') <> 1
    OR COUNT_IF(reporting_date = '2024-10-31' AND NOT is_in_numerator
        AND latest_review_date = '2023-06-30' AND latest_record_date IS NULL) <> 1
