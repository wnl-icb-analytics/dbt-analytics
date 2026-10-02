{{ config(tags=['monthly-full', 'nice-history']) }}

{% set calculation_233 = nice_ind233('by_month') %}
{% set calculation_233 = calculation_233 | replace(ref('int_nice_reference_population_by_month') | string, 'synthetic_population') %}
{% set calculation_233 = calculation_233 | replace(ref('int_ckd_profile_by_month') | string, 'synthetic_profile') %}
{% set calculation_234 = nice_ind234('by_month') %}
{% set calculation_234 = calculation_234 | replace(ref('int_nice_reference_population_by_month') | string, 'synthetic_population') %}
{% set calculation_234 = calculation_234 | replace(ref('int_ckd_profile_by_month') | string, 'synthetic_profile') %}

WITH synthetic_keys AS (
    SELECT -9405 AS person_id, column1::DATE AS reporting_date
    FROM VALUES ('2026-09-30'), ('2026-10-31')
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
        reporting_date,
        '2025-09-30'::DATE AS ckd_diagnosis_date,
        40::FLOAT AS latest_egfr_value,
        20::FLOAT AS latest_acr_value,
        TRUE AS has_egfr_pair_before_diagnosis,
        '2025-09-01'::DATE AS second_egfr_before_diagnosis_date,
        TRUE AS has_egfr_within_90_days_of_diagnosis,
        '2025-09-01'::DATE AS egfr_within_90_days_of_diagnosis_date,
        TRUE AS has_acr_within_90_days_of_diagnosis,
        '2025-09-02'::DATE AS acr_within_90_days_of_diagnosis_date
    FROM synthetic_keys
),
actual AS (
    SELECT indicator_id, reporting_date, is_in_numerator FROM ({{ calculation_233 }})
    UNION ALL
    SELECT indicator_id, reporting_date, is_in_numerator FROM ({{ calculation_234 }})
)
SELECT COUNT(*) AS rows_total
FROM actual
HAVING COUNT(*) <> 2
    OR COUNT_IF(reporting_date = '2026-09-30' AND is_in_numerator) <> 2
