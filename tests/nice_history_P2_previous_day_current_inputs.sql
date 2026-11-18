{{ config(tags=['monthly-full', 'nice-history']) }}

{% set calculation = nice_ind229('current') %}
{% set calculation = calculation | replace(ref('dim_person_active_patients') | string, 'synthetic_active') %}
{% set calculation = calculation | replace(ref('dim_person_age') | string, 'synthetic_age') %}
{% set calculation = calculation | replace(ref('dim_person_demographics') | string, 'synthetic_demographics') %}
{% set calculation = calculation | replace(ref('int_cvd_risk_profile') | string, 'stored_cvd') %}
{% set calculation = calculation | replace(ref('int_nice_therapy_evidence') | string, 'stored_therapy') %}

WITH synthetic_active AS (
    SELECT -9699::NUMBER AS person_id,
        'SYNTHETIC' AS current_practice_code,
        'Synthetic practice' AS current_practice_name
),
synthetic_age AS (
    SELECT -9699::NUMBER AS person_id, 50 AS age,
        '1976-01-01'::DATE AS birth_date_approx
),
synthetic_demographics AS (
    SELECT -9699::NUMBER AS person_id, 'Female' AS gender
),
stored_cvd AS (
    SELECT -9699::NUMBER AS person_id,
        DATEADD(day, -1, CURRENT_DATE())::DATE AS reporting_date,
        10 AS latest_cvd_risk_score,
        FALSE AS has_cvd_including_haemorrhagic_stroke
),
stored_therapy AS (
    SELECT -9699::NUMBER AS person_id,
        DATEADD(day, -1, CURRENT_DATE())::DATE AS reporting_date,
        DATEADD(day, -1, CURRENT_DATE())::DATE AS latest_lipid_lowering_order_date,
        'STATIN' AS latest_lipid_lowering_class,
        'Synthetic statin' AS latest_lipid_lowering_product,
        TRUE AS is_latest_lipid_lowering_statin
),
expected AS (
    SELECT -9699::NUMBER AS person_id,
        CURRENT_DATE()::DATE AS reporting_date,
        DATEADD(day, -1, CURRENT_DATE())::DATE AS latest_lipid_lowering_order_date,
        TRUE AS is_in_numerator,
        'ACHIEVED'::VARCHAR AS indicator_status
),
actual AS ({{ calculation }}),
actual_occurrences AS (
    SELECT person_id, reporting_date, latest_lipid_lowering_order_date,
        is_in_numerator, indicator_status, COUNT(*) AS occurrences
    FROM actual
    GROUP BY ALL
),
expected_occurrences AS (
    SELECT person_id, reporting_date, latest_lipid_lowering_order_date,
        is_in_numerator, indicator_status, COUNT(*) AS occurrences
    FROM expected
    GROUP BY ALL
),
differences AS (
    (SELECT * FROM actual_occurrences EXCEPT SELECT * FROM expected_occurrences)
    UNION ALL
    (SELECT * FROM expected_occurrences EXCEPT SELECT * FROM actual_occurrences)
)
SELECT COUNT(*) AS failures
FROM differences
HAVING COUNT(*) > 0
