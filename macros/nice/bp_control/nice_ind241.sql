{% macro nice_ind241(reference='current') %}
{#-
    Calculate NICE IND241 from register membership and paired BP evidence.
    Args: reference is current or by_month.
    Returns: the IND241 detail columns, one eligible person per reporting_date.
-#}
-- NICE IND241: coronary heart disease BP control for people aged 79 years and under.
WITH indicator_population AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name
    FROM ({{ nice_register('CHD', reference) }}) AS register
    INNER JOIN ({{ nice_reference_population(reference) }}) AS population
        ON register.person_id = population.person_id
        AND register.reporting_date = population.reporting_date
    WHERE population.age <= 79
),

assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        bp.latest_bp_date,
        bp.is_valid_bp,
        bp.latest_systolic_value,
        bp.latest_diastolic_value,
        bp.is_home_bp_event,
        bp.is_abpm_bp_event,
        bp.applied_measurement_context,
        COALESCE(
            bp.latest_bp_date::DATE
                BETWEEN DATEADD(month, -12, population.reporting_date) AND population.reporting_date,
            FALSE
        ) AS is_bp_recorded_in_last_12m,
        CASE
            WHEN bp.person_id IS NULL THEN NULL
            WHEN bp.applied_measurement_context = 'HBPM_ABPM' THEN 135
            ELSE 140
        END AS indicator_systolic_threshold,
        CASE
            WHEN bp.person_id IS NULL THEN NULL
            WHEN bp.applied_measurement_context = 'HBPM_ABPM' THEN 85
            ELSE 90
        END AS indicator_diastolic_threshold
    FROM indicator_population AS population
    LEFT JOIN {{ nice_ref('int_nice_blood_pressure_latest', reference) }} AS bp
        ON population.person_id = bp.person_id
        AND population.reporting_date = bp.reporting_date
),

status AS (
    SELECT
        *,
        COALESCE(
            is_valid_bp
            AND latest_systolic_value < indicator_systolic_threshold
            AND latest_diastolic_value < indicator_diastolic_threshold,
            FALSE
        ) AS is_latest_bp_within_indicator_target
    FROM assessed
)

SELECT
    person_id,
    'IND241' AS indicator_id,
    'Angina and coronary heart disease: blood pressure (79 years and under)' AS indicator_name,
    reporting_date,
    DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age,
    {{ nice_practice_columns('status', reference) }},
    'Coronary heart disease, aged 79 or under' AS condition_name,
    latest_bp_date,
    is_valid_bp,
    latest_systolic_value,
    latest_diastolic_value,
    is_home_bp_event,
    is_abpm_bp_event,
    applied_measurement_context,
    indicator_systolic_threshold,
    indicator_diastolic_threshold,
    TRUE AS is_in_denominator,
    is_bp_recorded_in_last_12m,
    is_latest_bp_within_indicator_target,
    is_bp_recorded_in_last_12m
        AND is_latest_bp_within_indicator_target AS is_in_numerator,
    CASE
        WHEN NOT is_bp_recorded_in_last_12m THEN 'NOT_RECORDED_IN_PERIOD'
        WHEN NOT is_valid_bp THEN 'NOT_ASSESSABLE'
        WHEN is_latest_bp_within_indicator_target THEN 'ACHIEVED'
        ELSE 'ABOVE_TARGET'
    END AS indicator_status
FROM status
{% endmacro %}
