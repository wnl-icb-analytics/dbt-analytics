{{ config(materialized='table') }}

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
    reporting_date,
    measurement_period_start,
    age,
    condition_name,
    current_practice_code,
    current_practice_name,
    latest_vaccination_date,
    is_laiv,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref(indicator_model) }}
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
