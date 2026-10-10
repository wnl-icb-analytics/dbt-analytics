{{ config(tags=['monthly-full', 'nice-history']) }}

{% set calculation = nice_ind157('by_month') %}
{% set calculation = calculation | replace(ref('int_nice_reference_population_by_month') | string, 'synthetic_population') %}
{% set calculation = calculation | replace(ref('int_nice_ltc_population_by_month') | string, 'synthetic_ltc') %}
{% set calculation = calculation | replace(ref('int_nice_smoking_evidence_by_month') | string, 'synthetic_smoking') %}

WITH synthetic_population AS (
    SELECT
        -7710::NUMBER AS person_id,
        column1::DATE AS reporting_date,
        40 AS age,
        '1986-01-15'::DATE AS birth_date_approx,
        'Female' AS gender,
        'SYNTHETIC' AS practice_code,
        'Synthetic practice' AS practice_name
    FROM VALUES ('2026-08-31'), ('2026-09-30'), ('2026-10-31')
),
synthetic_ltc AS (
    SELECT
        person_id,
        reporting_date,
        birth_date_approx,
        TRUE AS has_chd,
        FALSE AS has_pad,
        FALSE AS has_stroke_tia,
        FALSE AS has_hypertension,
        FALSE AS has_diabetes,
        FALSE AS has_copd,
        FALSE AS has_ckd,
        FALSE AS has_asthma
    FROM synthetic_population
),
synthetic_smoking AS (
    SELECT
        person_id,
        reporting_date,
        IFF(reporting_date = '2026-10-31', 'Ex-Smoker', 'Current Smoker') AS latest_smoking_status,
        reporting_date AS latest_smoking_status_date,
        NULL::DATE AS latest_never_smoked_date,
        '2025-08-31'::DATE AS latest_smoking_intervention_date
    FROM synthetic_population
),
actual AS ({{ calculation }})
SELECT COUNT(*) AS failure_count
FROM actual
HAVING COUNT(*) <> 2
    OR COUNT_IF(reporting_date = '2026-08-31' AND is_in_numerator
        AND latest_record_date = '2025-08-31') <> 1
    OR COUNT_IF(reporting_date = '2026-09-30' AND NOT is_in_numerator
        AND latest_record_date IS NULL AND indicator_status = 'NOT_RECORDED_IN_PERIOD') <> 1
