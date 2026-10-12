{% macro nice_ind165(reference='current') %}
{#-
    Calculate NICE IND165 at each reference date using its existing rule.
    Args: reference is current or by_month.
    Returns: the indicator detail columns, one eligible person per reporting_date.
-#}
-- NICE IND165: https://www.nice.org.uk/indicators/ind165
-- Last HbA1c in 12 months at or below 58 mmol/mol, whole diabetes register.
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
    LEFT JOIN {{ nice_ref('int_nice_hba1c_evidence', reference) }} AS evidence
        ON population.person_id = evidence.person_id
        AND population.reporting_date = evidence.reporting_date
    WHERE TRUE
        -- Fructosamine only excludes when no HbA1c test exists in the same period.
        AND NOT (
            COALESCE(evidence.latest_fructosamine_date >= DATEADD(month, -12, population.reporting_date), FALSE)
            AND NOT COALESCE(evidence.latest_hba1c_date >= DATEADD(month, -12, population.reporting_date), FALSE)
        )
        -- Maximum tolerated treatment expires after the inclusive 12-month window.
        AND NOT COALESCE(evidence.latest_dmmax_date >= DATEADD(month, -12, population.reporting_date), FALSE)
),

assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        {{ nice_practice_columns('population', reference) }},
        population.latest_frailty_severity,
        hba.latest_hba1c_observation_id,
        hba.latest_hba1c_date,
        hba.latest_hba1c_value,
        COALESCE(hba.is_latest_hba1c_valid, FALSE) AS is_latest_hba1c_valid,
        58 AS indicator_threshold
    FROM indicator_population AS population
    -- Value-free and invalid latest tests remain unassessable, without fallback.
    -- The paired evidence prefers valid companions only on the same latest day.
    LEFT JOIN {{ nice_ref('int_nice_hba1c_evidence', reference) }} AS hba
        ON population.person_id = hba.person_id
        AND population.reporting_date = hba.reporting_date
        -- Mask every stale result field, matching the current in-window selection.
        AND hba.latest_hba1c_date >= DATEADD(month, -12, population.reporting_date)
)

SELECT
    person_id,
    'IND165' AS indicator_id,
    'Diabetes: IFCC-HbA1c 58mmol/mol or less' AS indicator_name,
    'The percentage of patients with diabetes, on the register, in whom the last IFCC-HbA1c is 58 mmol/mol or less in the preceding 12 months.' AS indicator_description,
    reporting_date AS reporting_date,
    DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age,
    'Diabetes' AS denominator_description,
    {{ nice_practice_columns(none, reference) }},
    latest_frailty_severity,
    latest_hba1c_observation_id,
    latest_hba1c_date AS latest_record_date,
    latest_hba1c_value,
    is_latest_hba1c_valid,
    indicator_threshold,
    'mmol/mol' AS threshold_unit,
    TRUE AS is_in_denominator,
    latest_hba1c_observation_id IS NOT NULL AS is_hba1c_recorded_in_period,
    COALESCE(is_latest_hba1c_valid AND latest_hba1c_value <= indicator_threshold, FALSE) AS is_in_numerator,
    CASE
        WHEN latest_hba1c_observation_id IS NULL THEN 'NOT_RECORDED_IN_PERIOD'
        WHEN NOT is_latest_hba1c_valid THEN 'NOT_ASSESSABLE'
        WHEN latest_hba1c_value <= indicator_threshold THEN 'ACHIEVED'
        ELSE 'ABOVE_TARGET'
    END AS indicator_status
FROM assessed
{% endmacro %}
