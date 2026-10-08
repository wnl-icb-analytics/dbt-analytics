{% macro nice_childhood_immunisation_union(reference='current') %}
{#-
    Combine the explicit NICE childhood members at their shared detail grain.
    Args: reference is current or by_month; every referenced member must exist.
    Returns: the childhood family columns, one person/indicator per reporting_date.
-#}
{% if reference not in ['current', 'by_month'] %}
    {{ exceptions.raise_compiler_error('Unsupported NICE reference: ' ~ reference) }}
{% endif %}
-- Common long-form interface for the NICE childhood immunisation indicator views.
{% set indicator_models = [
    'fct_person_childhood_immunisation_ind215',
    'fct_person_childhood_immunisation_ind216',
    'fct_person_childhood_immunisation_ind217',
    'fct_person_childhood_immunisation_ind218',
    'fct_person_childhood_immunisation_ind224',
    'fct_person_childhood_immunisation_ind225',
    'fct_person_childhood_immunisation_ind226'
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
    birth_date_approx,
    milestone_date,
    doses_in_window,
    {% if indicator_model == 'fct_person_childhood_immunisation_ind226' %}
    primary_doses_by_12_months,
    booster_doses_12_to_18_months,
    {% else %}
    NULL::NUMBER AS primary_doses_by_12_months,
    NULL::NUMBER AS booster_doses_12_to_18_months,
    {% endif %}
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref(indicator_model if reference == 'current' else indicator_model ~ '_by_month') }}
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
{% endmacro %}
