{% macro nice_ind249(reference='current') %}
{#-
    Calculate NICE IND249 at each reference date using its existing rule.
    Args: reference is current or by_month.
    Returns: the indicator detail columns, one eligible person per reporting_date.
-#}
-- NICE IND249: https://www.nice.org.uk/indicators/ind249
-- Shared BP selection prefers valid pairs on the latest complete date, then lowest systolic/diastolic; unlinked components use daily maxima.
-- Last BP in 12 months below 140/90 clinic or 135/85 home, diabetes register aged 17 to 79 without moderate or severe frailty.
WITH indicator_population AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        frailty.latest_frailty_severity
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN ({{ nice_register('DM', reference) }}) AS diabetes
        ON population.person_id = diabetes.person_id
        AND population.reporting_date = diabetes.reporting_date
    LEFT JOIN ({{ nice_register('FRAIL', reference) }}) AS frailty
        ON population.person_id = frailty.person_id
        AND population.reporting_date = frailty.reporting_date
    WHERE population.age BETWEEN 17 AND 79
        AND COALESCE(frailty.latest_frailty_severity, 'None') NOT IN ('Moderate', 'Severe')
),

assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        {{ nice_practice_columns('population', reference) }},
        population.latest_frailty_severity,
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
    'IND249' AS indicator_id,
    'Diabetes: blood pressure (without moderate or severe frailty)' AS indicator_name,
    'The percentage of patients with diabetes on the register, aged 79 years and under without moderate or severe frailty, in whom the last blood pressure reading (measured in the preceding 12 months) is less than 135/85 mmHg if using ambulatory or home monitoring, or less than 140/90 mmHg if measured in clinic.' AS indicator_description,
    reporting_date AS reporting_date,
    DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age,
    'Diabetes, aged 17 to 79, no moderate or severe frailty' AS denominator_description,
    {{ nice_practice_columns(none, reference) }},
    latest_frailty_severity,
    latest_bp_date,
    is_valid_bp,
    CASE WHEN is_bp_recorded_in_last_12m THEN latest_bp_date END AS latest_record_date,
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
