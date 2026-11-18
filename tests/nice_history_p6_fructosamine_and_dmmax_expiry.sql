{{ config(tags=['monthly-full', 'nice-history']) }}

{% set actual_136 = nice_ind136('by_month') %}
{% set actual_136 = actual_136 | replace(nice_reference_population('by_month'), 'SELECT * FROM synthetic_population') %}
{% set actual_136 = actual_136 | replace(nice_register('DM', 'by_month'), 'SELECT * FROM synthetic_dm') %}
{% set actual_136 = actual_136 | replace(nice_register('FRAIL', 'by_month'), 'SELECT * FROM synthetic_frail') %}
{% set actual_136 = actual_136 | replace(nice_ref('int_nice_hba1c_evidence', 'by_month') | string, 'synthetic_hba') %}

WITH synthetic_keys AS (
    SELECT
        column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        column3::NUMBER AS age
    FROM VALUES
        (602, '2024-01-31', 40),
        (602, '2024-02-29', 40),
        (603, '2024-01-31', 40),
        (603, '2024-02-29', 40),
        (604, '2024-01-31', 40)
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
        (602, '2024-01-31'),
        (602, '2024-02-29'),
        (603, '2024-01-31'),
        (603, '2024-02-29'),
        (604, '2024-01-31')
),

synthetic_frail AS (
    SELECT
        column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        column3::VARCHAR AS latest_frailty_severity
    FROM VALUES
        (0, '2024-01-31', NULL)
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
        (602, '2024-01-31', NULL, NULL, NULL, FALSE, '2023-01-31', NULL),
        (602, '2024-02-29', NULL, NULL, NULL, FALSE, '2023-01-31', NULL),
        (603, '2024-01-31', 'a', '2024-01-01', 75, TRUE, NULL, '2023-01-31'),
        (603, '2024-02-29', 'a', '2024-01-01', 75, TRUE, NULL, '2023-01-31'),
        (604, '2024-01-31', 'b', '2024-01-01', NULL, FALSE, '2023-01-31', NULL)
),

actual_136 AS ({{ actual_136 }})

SELECT COUNT(*) AS failure_count
FROM actual_136
HAVING COUNT(*) <> 3
    OR COUNT_IF(person_id = 602 AND reporting_date = '2024-02-29'
        AND indicator_status = 'NOT_RECORDED_IN_PERIOD') <> 1
    OR COUNT_IF(person_id = 603 AND reporting_date = '2024-02-29'
        AND indicator_status = 'ACHIEVED') <> 1
    OR COUNT_IF(person_id = 604 AND indicator_status = 'NOT_ASSESSABLE'
        AND is_hba1c_recorded_in_period AND NOT is_in_numerator) <> 1
