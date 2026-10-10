{{ config(tags=['monthly-full', 'nice-history']) }}

{% set calculation_130 = nice_ind130('by_month') %}
{% set calculation_130 = calculation_130 | replace(ref('int_nice_reference_population_by_month') | string, 'synthetic_population') %}
{% set calculation_130 = calculation_130 | replace(ref('int_ckd_profile_by_month') | string, 'synthetic_profile') %}
{% set calculation_263 = nice_ind263('by_month') %}
{% set calculation_263 = calculation_263 | replace(ref('int_nice_reference_population_by_month') | string, 'synthetic_population') %}
{% set calculation_263 = calculation_263 | replace(ref('int_ckd_profile_by_month') | string, 'synthetic_profile') %}
{% set calculation_324 = nice_ind324('by_month') %}
{% set calculation_324 = calculation_324 | replace(ref('int_nice_reference_population_by_month') | string, 'synthetic_population') %}
{% set calculation_324 = calculation_324 | replace(ref('int_ckd_profile_by_month') | string, 'synthetic_profile') %}

WITH synthetic_keys AS (
    SELECT -9402 AS person_id, column1::DATE AS reporting_date
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
        80::FLOAT AS latest_acr_value,
        40::FLOAT AS latest_egfr_value,
        TRUE AS has_hypertension,
        TRUE AS has_proteinuria,
        FALSE AS has_diabetes,
        FALSE AS is_ace_inhibitor_contraindicated,
        FALSE AS is_arb_contraindicated,
        'Type 2'::VARCHAR AS diabetes_type,
        '2026-03-30'::DATE AS latest_ras_order_date,
        '2026-03-30'::DATE AS latest_sglt2_order_date,
        '2026-03-30'::DATE AS latest_ras_order_before_last_sglt2_date
    FROM synthetic_keys
),
actual AS (
    SELECT indicator_id, reporting_date, latest_record_date, is_in_numerator, indicator_status
    FROM ({{ calculation_130 }})
    UNION ALL
    SELECT indicator_id, reporting_date, latest_record_date, is_in_numerator, indicator_status
    FROM ({{ calculation_263 }})
    UNION ALL
    SELECT indicator_id, reporting_date, latest_record_date, is_in_numerator, indicator_status
    FROM ({{ calculation_324 }})
)
SELECT COUNT(*) AS rows_total
FROM actual
HAVING COUNT(*) <> 6
    OR COUNT_IF(reporting_date = '2026-09-30'
        AND latest_record_date = '2026-03-30' AND is_in_numerator
        AND indicator_status = 'ACHIEVED') <> 3
    OR COUNT_IF(reporting_date = '2026-10-31'
        AND latest_record_date IS NULL AND NOT is_in_numerator
        AND indicator_status = 'NOT_TREATED_IN_PERIOD') <> 3
