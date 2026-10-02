{% macro nice_ind264(reference='current') %}
{#-
    Calculate NICE IND264 for eligible CKD members at each reference date.
    Args: reference is current or by_month.
    Returns: the IND264 detail columns, one person per reporting_date.
-#}
-- NICE IND264: https://www.nice.org.uk/indicators/ind264
-- Last BP in 12 months below 130/80 clinic or 125/75 home for people on the CKD register with a latest ACR of 70 mg/mmol or more, without moderate or severe frailty.
WITH indicator_population AS (
    SELECT
        profile.person_id,
        profile.reporting_date,
        profile.latest_acr_value,
        profile.latest_frailty_severity,
        population.age,
        population.practice_code,
        population.practice_name
    FROM {{ nice_ref('int_ckd_profile', reference) }} AS profile
    INNER JOIN ({{ nice_reference_population(reference) }}) AS population
        ON profile.person_id = population.person_id
        AND profile.reporting_date = population.reporting_date
    WHERE profile.latest_acr_value >= 70
        AND COALESCE(profile.latest_frailty_severity, 'None') NOT IN ('Moderate', 'Severe')
),

assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        population.latest_frailty_severity,
        population.latest_acr_value,
        bp.latest_bp_date,
        bp.is_valid_bp,
        bp.latest_systolic_value,
        bp.latest_diastolic_value,
        bp.applied_measurement_context,
        COALESCE(bp.latest_bp_date::DATE
            BETWEEN DATEADD(month, -12, population.reporting_date)
                AND population.reporting_date, FALSE) AS is_bp_recorded_in_last_12m,
        CASE
            WHEN bp.person_id IS NULL THEN NULL
            WHEN bp.applied_measurement_context = 'HBPM_ABPM' THEN 125
            ELSE 130
        END AS indicator_systolic_threshold,
        CASE
            WHEN bp.person_id IS NULL THEN NULL
            WHEN bp.applied_measurement_context = 'HBPM_ABPM' THEN 75
            ELSE 80
        END AS indicator_diastolic_threshold
    FROM indicator_population AS population
    LEFT JOIN {{ nice_ref('int_nice_blood_pressure_latest', reference) }} AS bp
        ON population.person_id = bp.person_id
        AND population.reporting_date = bp.reporting_date
),

status AS (
    SELECT
        *,
        COALESCE(is_valid_bp AND latest_systolic_value < indicator_systolic_threshold
            AND latest_diastolic_value < indicator_diastolic_threshold, FALSE) AS is_latest_bp_within_indicator_target
    FROM assessed
)

SELECT
    person_id,
    'IND264' AS indicator_id,
    'Kidney conditions: CKD and blood pressure when ACR 70 or more' AS indicator_name,
    reporting_date,
    DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age,
    'CKD with ACR 70 mg/mmol or more, without moderate or severe frailty' AS condition_name,
    {{ nice_practice_columns('result', reference) }},
    latest_frailty_severity,
    latest_acr_value,
    latest_bp_date,
    is_valid_bp,
    CASE WHEN is_bp_recorded_in_last_12m THEN latest_bp_date END AS latest_record_date,
    latest_systolic_value,
    latest_diastolic_value,
    applied_measurement_context,
    indicator_systolic_threshold,
    indicator_diastolic_threshold,
    TRUE AS is_in_denominator,
    is_bp_recorded_in_last_12m AND is_latest_bp_within_indicator_target AS is_in_numerator,
    CASE
        WHEN NOT is_bp_recorded_in_last_12m THEN 'NOT_RECORDED_IN_PERIOD'
        WHEN NOT is_valid_bp THEN 'NOT_ASSESSABLE'
        WHEN is_latest_bp_within_indicator_target THEN 'ACHIEVED'
        ELSE 'ABOVE_TARGET'
    END AS indicator_status
FROM status AS result

{% endmacro %}
