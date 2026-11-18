{% macro nice_bp_control_union(reference='current') %}
{#-
    Combine NICE IND239 to IND246 with the existing BP detail projection.
    Args: reference is current or by_month.
    Returns: one person/indicator per reporting_date.
-#}
{% if reference not in ['current', 'by_month'] %}
    {{ exceptions.raise_compiler_error('Unsupported NICE reference: ' ~ reference) }}
{% endif %}
-- Common long-form interface for the NICE BP control indicator views.
{% set indicator_models = [
    'fct_person_bp_control_hypertension_ind239',
    'fct_person_bp_control_hypertension_ind240',
    'fct_person_bp_control_chd_ind241',
    'fct_person_bp_control_chd_ind242',
    'fct_person_bp_control_stroke_tia_ind243',
    'fct_person_bp_control_stroke_tia_ind244',
    'fct_person_bp_control_pad_ind245',
    'fct_person_bp_control_pad_ind246'
] %}

{% for indicator_model in indicator_models %}
SELECT
    person_id,
    indicator_id,
    indicator_name,
    indicator_description,
    reporting_date,
    measurement_period_start,
    age,
    {{ nice_practice_columns(none, reference) }},
    denominator_description,
    latest_bp_date,
    is_valid_bp,
    latest_systolic_value,
    latest_diastolic_value,
    is_home_bp_event,
    is_abpm_bp_event,
    applied_measurement_context,
    indicator_systolic_threshold,
    indicator_diastolic_threshold,
    is_in_denominator,
    is_bp_recorded_in_last_12m,
    is_latest_bp_within_indicator_target,
    is_in_numerator,
    indicator_status
FROM {{ ref(indicator_model if reference == 'current' else indicator_model ~ '_by_month') }}
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
{% endmacro %}
