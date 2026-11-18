{{ config(tags=['monthly-full', 'nice-history']) }}

{% set actual_165 = nice_ind165('by_month') %}
{% set actual_165 = actual_165 | replace(nice_reference_population('by_month'), 'SELECT * FROM synthetic_population') %}
{% set actual_165 = actual_165 | replace(nice_register('DM', 'by_month'), 'SELECT * FROM synthetic_dm') %}
{% set actual_165 = actual_165 | replace(nice_register('FRAIL', 'by_month'), 'SELECT * FROM synthetic_frail') %}
{% set actual_165 = actual_165 | replace(nice_ref('int_nice_hba1c_evidence', 'by_month') | string, 'synthetic_hba') %}
{% set actual_179 = nice_ind179('by_month') %}
{% set actual_179 = actual_179 | replace(nice_reference_population('by_month'), 'SELECT * FROM synthetic_population') %}
{% set actual_179 = actual_179 | replace(nice_register('DM', 'by_month'), 'SELECT * FROM synthetic_dm') %}
{% set actual_179 = actual_179 | replace(nice_register('FRAIL', 'by_month'), 'SELECT * FROM synthetic_frail') %}
{% set actual_179 = actual_179 | replace(nice_ref('int_nice_hba1c_evidence', 'by_month') | string, 'synthetic_hba') %}
{% set actual_180 = nice_ind180('by_month') %}
{% set actual_180 = actual_180 | replace(nice_reference_population('by_month'), 'SELECT * FROM synthetic_population') %}
{% set actual_180 = actual_180 | replace(nice_register('DM', 'by_month'), 'SELECT * FROM synthetic_dm') %}
{% set actual_180 = actual_180 | replace(nice_register('FRAIL', 'by_month'), 'SELECT * FROM synthetic_frail') %}
{% set actual_180 = actual_180 | replace(nice_ref('int_nice_hba1c_evidence', 'by_month') | string, 'synthetic_hba') %}

WITH synthetic_keys AS (
    SELECT
        column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        column3::NUMBER AS age
    FROM VALUES
        (605, '2024-01-31', 40),
        (605, '2024-02-29', 40)
),

synthetic_population AS (
    SELECT
        person_id,
        reporting_date,
        age,
        '1980-01-01'::DATE AS birth_date_approx,
        'Female'::VARCHAR AS gender,
        'SYNTHETIC'::VARCHAR AS practice_code,
        'Synthetic practice'::VARCHAR AS practice_name
    FROM synthetic_keys
),

synthetic_dm AS (
    SELECT
        column1::NUMBER AS person_id,
        column2::DATE AS reporting_date
    FROM VALUES
        (605, '2024-01-31'),
        (605, '2024-02-29')
),

synthetic_frail AS (
    SELECT
        column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        column3::VARCHAR AS latest_frailty_severity
    FROM VALUES
        (605, '2024-01-31', 'Mild'),
        (605, '2024-02-29', 'Moderate')
),

synthetic_hba AS (
    SELECT
        column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        column3::VARCHAR AS latest_hba1c_observation_id,
        column4::DATE AS latest_hba1c_date,
        column5::NUMBER AS latest_hba1c_value,
        column6::BOOLEAN AS is_latest_hba1c_valid,
        column7::DATE AS latest_fructosamine_date,
        column8::DATE AS latest_dmmax_date
    FROM VALUES
        (605, '2024-01-31', 'a', '2024-01-01', 58, TRUE, NULL, NULL),
        (605, '2024-02-29', 'b', '2024-02-01', 75, TRUE, NULL, NULL)
),

actual_165 AS ({{ actual_165 }}),

actual_179 AS ({{ actual_179 }}),

actual_180 AS ({{ actual_180 }}),

actual AS (SELECT * FROM actual_165 UNION ALL SELECT * FROM actual_179 UNION ALL SELECT * FROM actual_180)

SELECT COUNT(*) AS failure_count
FROM actual
HAVING COUNT(*) <> 4
    OR COUNT_IF(indicator_id = 'IND179' AND reporting_date = '2024-01-31'
        AND latest_frailty_severity = 'Mild' AND is_in_numerator) <> 1
    OR COUNT_IF(indicator_id = 'IND180' AND reporting_date = '2024-02-29'
        AND latest_frailty_severity = 'Moderate' AND is_in_numerator) <> 1
    OR COUNT_IF(indicator_id = 'IND165' AND reporting_date = '2024-02-29'
        AND latest_frailty_severity = 'Moderate' AND indicator_status = 'ABOVE_TARGET') <> 1
