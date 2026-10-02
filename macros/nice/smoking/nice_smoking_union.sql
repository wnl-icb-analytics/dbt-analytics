{% macro nice_smoking_union(reference='current') %}
{#- Combine the smoking indicators at person/indicator/reporting-date grain. -#}
-- Common long-form interface for the NICE smoking indicator views.
{% set indicator_models = [
    'fct_person_smoking_ind156',
    'fct_person_smoking_ind157',
    'fct_person_smoking_ind97',
    'fct_person_smoking_ind98',
    'fct_person_smoking_ind99'
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
    latest_smoking_status,
    latest_smoking_status_date,
    latest_smoking_intervention_date,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref(indicator_model if reference == 'current' else indicator_model ~ '_by_month') }}
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}

{% endmacro %}
