{{ config(tags=['monthly-full', 'nice-history']) }}

{% set calculation = nice_ind265('current') %}
{% set calculation = calculation | replace(nice_reference_population('current') | string, 'SELECT * FROM synthetic_population') %}
{% set calculation = calculation | replace(ref('int_nice_ltc_population') | string, 'synthetic_profile') %}
{% set calculation = calculation | replace(ref('int_nice_review_evidence') | string, 'synthetic_reviews') %}

WITH synthetic_population AS (
    SELECT -9904::NUMBER AS person_id, CURRENT_DATE()::DATE AS reporting_date,
        40 AS age, 'SYNTHETIC' AS practice_code, 'Synthetic practice' AS practice_name
),
synthetic_profile AS (
    SELECT person_id, DATEADD(day, -1, reporting_date)::DATE AS reporting_date,
        TRUE AS has_learning_disability
    FROM synthetic_population
),
synthetic_reviews AS (
    SELECT person_id, DATEADD(day, -1, reporting_date)::DATE AS reporting_date,
        DATEADD(day, -10, reporting_date)::DATE AS latest_ld_health_check_date,
        DATEADD(day, -9, reporting_date)::DATE AS latest_ld_health_action_plan_date
    FROM synthetic_population
),
actual AS ({{ calculation }})
SELECT COUNT(*) AS rows_total
FROM actual
HAVING COUNT(*) <> 1
    OR COUNT_IF(reporting_date = CURRENT_DATE() AND is_in_numerator
        AND latest_record_date = DATEADD(day, -10, CURRENT_DATE())) <> 1
