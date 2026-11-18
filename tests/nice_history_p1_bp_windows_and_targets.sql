{{ config(tags=['monthly-full', 'nice-history']) }}

{% set registers = {239: 'HTN', 240: 'HTN', 241: 'CHD', 242: 'CHD', 243: 'STIA', 244: 'STIA', 245: 'PAD', 246: 'PAD'} %}
{% set queries = [] %}
{% for indicator, condition in registers.items() %}
    {% set calculation = context['nice_ind' ~ indicator]('by_month') %}
    {% set calculation = calculation | replace(nice_register(condition, 'by_month') | string, 'SELECT person_id, reporting_date FROM synthetic_population') %}
    {% set calculation = calculation | replace(nice_reference_population('by_month') | string, 'SELECT * FROM synthetic_population') %}
    {% set calculation = calculation | replace(nice_ref('int_nice_blood_pressure_latest', 'by_month') | string, 'synthetic_bp') %}
    {% do queries.append(calculation) %}
{% endfor %}

WITH synthetic_population AS (
    SELECT
        column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        column3::NUMBER AS age,
        'SYNTHETIC' AS practice_code,
        'Synthetic practice' AS practice_name
    FROM VALUES
        (-9391, '2026-09-30', 79),
        (-9391, '2026-10-31', 80),
        (-9392, '2026-09-30', 79),
        (-9393, '2026-09-30', 80),
        (-9394, '2026-09-30', 79),
        (-9395, '2026-09-30', 80),
        (-9396, '2026-09-30', 79),
        (-9397, '2026-09-30', 80),
        (-9398, '2026-09-30', NULL),
        (-9401, '2026-09-30', 80),
        (-9402, '2026-09-30', 79),
        (-9403, '2026-09-30', 80)
),
synthetic_bp AS (
    SELECT
        column1::NUMBER AS person_id,
        column2::DATE AS reporting_date,
        column3::DATE AS latest_bp_date,
        column4::NUMBER AS latest_systolic_value,
        column5::NUMBER AS latest_diastolic_value,
        column6::BOOLEAN AS is_valid_bp,
        column7::BOOLEAN AS is_home_bp_event,
        FALSE AS is_abpm_bp_event,
        IFF(column7, 'HBPM_ABPM', 'CLINIC') AS applied_measurement_context
    FROM VALUES
        (-9391, '2026-09-30', '2025-09-30', 139, 89, TRUE, FALSE),
        (-9391, '2026-10-31', '2025-09-30', 139, 89, TRUE, FALSE),
        (-9392, '2026-09-30', '2026-09-30', 140, 89, TRUE, FALSE),
        (-9393, '2026-09-30', '2026-09-30', 149, 90, TRUE, FALSE),
        (-9394, '2026-09-30', '2026-09-30', 135, 84, TRUE, TRUE),
        (-9395, '2026-09-30', '2026-09-30', 144, 84, TRUE, TRUE),
        (-9397, '2026-09-30', '2026-09-30', 10, 10, FALSE, FALSE),
        (-9401, '2026-09-30', '2026-09-30', 145, 84, TRUE, TRUE),
        (-9402, '2026-09-30', '2026-09-30', 134, 85, TRUE, TRUE),
        (-9403, '2026-09-30', '2026-09-30', 150, 89, TRUE, FALSE)
),
actual AS (
    {% for calculation in queries %}
    SELECT * FROM ({{ calculation }})
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
)
SELECT COUNT(*) AS rows_total
FROM actual
HAVING COUNT(*) <> 44
    OR COUNT_IF(person_id = -9391 AND reporting_date = '2026-09-30'
        AND indicator_systolic_threshold = 140 AND indicator_diastolic_threshold = 90
        AND is_in_numerator AND indicator_status = 'ACHIEVED') <> 4
    OR COUNT_IF(person_id = -9391 AND reporting_date = '2026-10-31'
        AND indicator_systolic_threshold = 150 AND indicator_diastolic_threshold = 90
        AND latest_bp_date = '2025-09-30' AND is_latest_bp_within_indicator_target
        AND NOT is_in_numerator AND indicator_status = 'NOT_RECORDED_IN_PERIOD') <> 4
    OR COUNT_IF(person_id IN (-9392, -9393, -9394, -9401, -9402, -9403)
        AND is_bp_recorded_in_last_12m AND NOT is_in_numerator
        AND indicator_status = 'ABOVE_TARGET') <> 24
    OR COUNT_IF(person_id = -9395 AND indicator_systolic_threshold = 145
        AND indicator_diastolic_threshold = 85 AND is_in_numerator
        AND indicator_status = 'ACHIEVED') <> 4
    OR COUNT_IF(person_id = -9396 AND latest_bp_date IS NULL AND is_valid_bp IS NULL
        AND indicator_systolic_threshold IS NULL AND indicator_diastolic_threshold IS NULL
        AND NOT is_in_numerator AND indicator_status = 'NOT_RECORDED_IN_PERIOD') <> 4
    OR COUNT_IF(person_id = -9397 AND NOT is_valid_bp AND NOT is_in_numerator
        AND indicator_status = 'NOT_ASSESSABLE') <> 4
    OR COUNT_IF(person_id = -9398) <> 0
