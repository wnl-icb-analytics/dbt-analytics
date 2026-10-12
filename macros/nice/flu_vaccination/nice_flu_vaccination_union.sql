{% macro nice_flu_vaccination_union(reference='current') %}
{#-
    Combine the explicit NICE flu vaccination members and detail fields.
    Args: reference is current or by_month; every referenced member must exist.
    Returns: one eligible person/indicator per reporting_date.
-#}
{% if reference not in ['current', 'by_month'] %}
    {{ exceptions.raise_compiler_error('Unsupported NICE reference: ' ~ reference) }}
{% endif %}
-- Common long-form interface for the NICE flu vaccination indicator views.
{% set indicator_models = [
    'fct_person_flu_vaccination_ind131',
    'fct_person_flu_vaccination_ind141',
    'fct_person_flu_vaccination_ind152',
    'fct_person_flu_vaccination_ind163',
    'fct_person_flu_vaccination_ind164'
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
    latest_vaccination_date,
    is_laiv,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref(indicator_model if reference == 'current' else indicator_model ~ '_by_month') }}
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
{% endmacro %}
