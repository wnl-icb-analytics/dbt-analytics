{{ config(tags=['monthly-full', 'nice-history']) }}

{% set calculation = nice_ind233('current') %}
{% set calculation = calculation | replace(nice_reference_population('current') | trim, 'SELECT * FROM synthetic_population') %}
{% set calculation = calculation | replace(ref('int_ckd_profile') | string, 'synthetic_profile') %}

WITH synthetic_keys AS (
    SELECT -9407 AS person_id, CURRENT_DATE()::DATE AS reporting_date
),
synthetic_population AS (
    SELECT
        person_id,
        reporting_date,
        60 AS age,
        '1966-01-01'::DATE AS birth_date_approx,
        'Female'::VARCHAR AS gender,
        'SYNTHETIC' AS practice_code,
        'Synthetic practice' AS practice_name
    FROM synthetic_keys
),
synthetic_profile AS (
    SELECT
        person_id,
        DATEADD(day, -1, reporting_date)::DATE AS reporting_date,
        DATEADD(month, -1, CURRENT_DATE())::DATE AS ckd_diagnosis_date,
        40::FLOAT AS latest_egfr_value,
        20::FLOAT AS latest_acr_value,
        TRUE AS has_egfr_pair_before_diagnosis,
        DATEADD(month, -2, CURRENT_DATE())::DATE AS second_egfr_before_diagnosis_date
    FROM synthetic_keys
),
actual AS ({{ calculation }})
SELECT COUNT(*) AS rows_total
FROM actual
HAVING COUNT(*) <> 1
    OR COUNT_IF(reporting_date = CURRENT_DATE() AND is_in_numerator) <> 1
