{% macro nice_childhood_immunisation_union(reference='current') %}
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
    reporting_date,
    measurement_period_start,
    age,
    condition_name,
    {{ 'current_practice_code' if reference == 'current' else 'practice_code' }},
    {{ 'current_practice_name' if reference == 'current' else 'practice_name' }},
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
