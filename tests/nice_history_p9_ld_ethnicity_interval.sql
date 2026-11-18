{{ config(tags=['monthly-full', 'nice-history']) }}

{% set calculation = nice_ind266('by_month') %}
{% set calculation = calculation | replace(nice_reference_population('by_month') | string, 'SELECT * FROM synthetic_population') %}
{% set calculation = calculation | replace(ref('int_nice_ltc_population_by_month') | string, 'synthetic_profile') %}
{% set calculation = calculation | replace(ref('int_nice_review_evidence_by_month') | string, 'synthetic_reviews') %}
{% set calculation = calculation | replace(ref('dim_person_demographics_historical') | string, 'synthetic_demographics') %}

WITH synthetic_population AS (
    SELECT
        column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        40 AS age,
        'SYNTHETIC' AS practice_code,
        'Synthetic practice' AS practice_name
    FROM VALUES
        (-9901, '2024-09-30'),
        (-9901, '2024-10-31'),
        (-9901, '2024-11-30'),
        (-9905, '2024-09-30')
),
synthetic_profile AS (
    SELECT person_id, reporting_date, TRUE AS has_learning_disability
    FROM synthetic_population
),
synthetic_reviews AS (
    SELECT
        person_id,
        reporting_date,
        IFF(reporting_date = '2024-11-30', '2024-11-01', '2023-09-30')::DATE AS latest_ld_health_check_date,
        IFF(reporting_date = '2024-11-30', '2024-10-31', '2023-09-30')::DATE AS latest_ld_health_action_plan_date
    FROM synthetic_population
),
synthetic_demographics AS (
    SELECT
        -9901::NUMBER AS person_id,
        column1::DATE AS effective_start_date,
        column2::DATE AS effective_end_date,
        column3::VARCHAR AS ethnicity_category
    FROM VALUES
        ('2024-09-30', '2024-10-31', 'White'),
        ('2024-10-31', NULL, 'Unknown')
),
actual AS ({{ calculation }})
SELECT COUNT(*) AS rows_total
FROM actual
HAVING COUNT(*) <> 4
    OR COUNT_IF(reporting_date = '2024-09-30'
        AND has_ethnicity_recorded AND is_in_numerator
        AND latest_record_date = '2023-09-30') <> 1
    OR COUNT_IF(reporting_date = '2024-10-31'
        AND NOT has_ethnicity_recorded AND NOT is_in_numerator
        AND latest_record_date IS NULL) <> 1
    OR COUNT_IF(reporting_date = '2024-11-30'
        AND NOT has_ethnicity_recorded AND NOT is_in_numerator
        AND latest_record_date = '2024-11-01') <> 1
    OR COUNT_IF(person_id = -9905 AND NOT has_ethnicity_recorded
        AND NOT is_in_numerator AND latest_record_date = '2023-09-30') <> 1
