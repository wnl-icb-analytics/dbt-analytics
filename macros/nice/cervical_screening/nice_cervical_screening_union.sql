{% macro nice_cervical_screening_union(reference='current') %}
{#-
    Combine the explicit NICE cervical screening members and detail fields.
    Args: reference is current or by_month; every referenced member must exist.
    Returns: one eligible person/indicator per reporting_date.
-#}
{% if reference not in ['current', 'by_month'] %}
    {{ exceptions.raise_compiler_error('Unsupported NICE reference: ' ~ reference) }}
{% endif %}
-- Common long-form interface for the NICE cervical screening indicator views.
{% set indicator_models = [
    'fct_person_cervical_screening_ind176',
    'fct_person_cervical_screening_ind177',
    'fct_person_cervical_screening_ind321'
] %}

{% for indicator_model in indicator_models %}
SELECT
    person_id,
    indicator_id,
    indicator_name,
    reporting_date,
    measurement_period_start,
    age,
    condition_name,
    {{ nice_practice_columns(none, reference) }},
    latest_completed_date,
    latest_screening_date,
    programme_status,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref(indicator_model if reference == 'current' else indicator_model ~ '_by_month') }}
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
{% endmacro %}
