{% macro nice_multimorbidity_union(reference='current') %}
{#-
    Combine NICE multimorbidity members with their existing detail columns.
    Args: reference is current or by_month.
    Returns: one eligible person/indicator per reporting_date.
-#}
{% if reference not in ['current', 'by_month'] %}
    {{ exceptions.raise_compiler_error('Unsupported NICE reference: ' ~ reference) }}
{% endif %}
-- Common long-form interface for the NICE multiple long-term conditions indicator views.
{% set indicator_models = [
    'fct_person_multimorbidity_ind207',
    'fct_person_multimorbidity_ind208'
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
    denominator_description,
    {{ nice_practice_columns(none, reference) }},
    ltc_count,
    multimorbidity_cluster_count,
    latest_frailty_severity,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref(indicator_model if reference == 'current' else indicator_model ~ '_by_month') }}
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}

{% endmacro %}
