{% macro nice_blood_pressure_for_target(reference, clinic_systolic, clinic_diastolic, home_systolic, home_diastolic) %}
{# Select one context on the latest eligible date for this indicator's target. #}
{% set achieves %}
    is_valid_bp
    AND systolic_value < IFF(bp_context_group = 'CLINIC', {{ clinic_systolic }}, {{ home_systolic }})
    AND diastolic_value < IFF(bp_context_group = 'CLINIC', {{ clinic_diastolic }}, {{ home_diastolic }})
{% endset %}
SELECT
    person_id,
    reporting_date,
    reading_date AS latest_bp_date,
    systolic_value AS latest_systolic_value,
    diastolic_value AS latest_diastolic_value,
    is_valid_bp,
    is_home_bp_event,
    is_abpm_bp_event,
    applied_measurement_context
FROM {{ nice_ref('int_nice_blood_pressure_control_latest_by_context', reference) }}
-- Achievement cannot make an older context replace the latest clinical date.
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY person_id, reporting_date
    ORDER BY reading_date DESC, {{ bp_control_context_order(achieves) }}
) = 1
{% endmacro %}
