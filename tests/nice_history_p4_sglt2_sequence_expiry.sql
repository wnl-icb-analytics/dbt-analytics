{{ config(tags=['monthly-full', 'nice-history']) }}

{% set calculation = nice_ind324('by_month') %}
{% set calculation = calculation | replace(ref('int_nice_reference_population_by_month') | string, 'synthetic_population') %}
{% set calculation = calculation | replace(ref('int_ckd_profile_by_month') | string, 'synthetic_profile') %}

WITH synthetic_keys AS (
    SELECT -9406 AS person_id, column1::DATE AS reporting_date
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
        30::FLOAT AS latest_egfr_value,
        25::FLOAT AS latest_acr_value,
        'Unknown'::VARCHAR AS diabetes_type,
        FALSE AS is_ace_inhibitor_contraindicated,
        FALSE AS is_arb_contraindicated,
        '2026-09-01'::DATE AS latest_ras_order_date,
        '2026-08-01'::DATE AS latest_sglt2_order_date,
        '2026-03-30'::DATE AS latest_ras_order_before_last_sglt2_date
    FROM synthetic_keys
),
actual AS ({{ calculation }})
SELECT COUNT(*) AS rows_total
FROM actual
HAVING COUNT(*) <> 2
    OR COUNT_IF(reporting_date = '2026-09-30'
        AND is_treatment_sequence_met AND is_in_numerator) <> 1
    OR COUNT_IF(reporting_date = '2026-10-31'
        AND latest_record_date = '2026-08-01' AND NOT is_treatment_sequence_met
        AND NOT is_in_numerator AND indicator_status = 'NOT_TREATED_IN_PERIOD') <> 1
