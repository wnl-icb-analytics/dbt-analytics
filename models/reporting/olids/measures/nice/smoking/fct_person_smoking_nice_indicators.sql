{{ config(materialized='table') }}

-- Common long-form interface for the NICE smoking indicator views.
{% set indicator_models = [
    'fct_person_smoking_ind156',
    'fct_person_smoking_ind157',
    'fct_person_smoking_ind97'
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
    current_practice_code,
    current_practice_name,
    latest_smoking_status,
    latest_smoking_status_date,
    latest_smoking_intervention_date,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref(indicator_model) }}
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
