{{ config(tags=['monthly-full', 'nice-history']) }}

{% set registers = {239: 'HTN', 240: 'HTN', 241: 'CHD', 242: 'CHD', 243: 'STIA', 244: 'STIA', 245: 'PAD', 246: 'PAD'} %}
{% set queries = [] %}
{% for indicator, condition in registers.items() %}
    {% set calculation = context['nice_ind' ~ indicator]('current') %}
    {% set calculation = calculation | replace(nice_register(condition, 'current') | string, 'SELECT person_id, reporting_date FROM synthetic_population') %}
    {% set calculation = calculation | replace(nice_reference_population('current') | string, 'SELECT * FROM synthetic_population') %}
    {% set calculation = calculation | replace(ref('int_nice_blood_pressure_latest') | string, 'stored_bp') %}
    {% do queries.append(calculation) %}
{% endfor %}

WITH synthetic_population AS (
    SELECT
        column1::NUMBER AS person_id,
        CURRENT_DATE()::DATE AS reporting_date,
        column2::NUMBER AS age,
        'SYNTHETIC' AS practice_code,
        'Synthetic practice' AS practice_name
    FROM VALUES (-9399, 79), (-9400, 80)
),
stored_bp AS (
    SELECT
        person_id,
        DATEADD(day, -1, CURRENT_DATE())::DATE AS reporting_date,
        DATEADD(day, -2, CURRENT_DATE())::DATE AS latest_bp_date,
        134::NUMBER AS latest_systolic_value,
        84::NUMBER AS latest_diastolic_value,
        TRUE AS is_valid_bp,
        TRUE AS is_home_bp_event,
        FALSE AS is_abpm_bp_event,
        'HBPM_ABPM' AS applied_measurement_context
    FROM synthetic_population
),
actual AS (
    {% for calculation in queries %}
    SELECT * FROM ({{ calculation }})
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
)
SELECT COUNT(*) AS rows_total
FROM actual
HAVING COUNT(*) <> 8
    OR COUNT_IF(reporting_date = CURRENT_DATE() AND is_in_numerator
        AND indicator_status = 'ACHIEVED' AND current_practice_code = 'SYNTHETIC'
        AND latest_bp_date = DATEADD(day, -2, CURRENT_DATE())) <> 8
