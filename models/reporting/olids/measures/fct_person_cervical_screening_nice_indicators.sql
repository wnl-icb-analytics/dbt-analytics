{{ config(materialized='table') }}

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
    current_practice_code,
    current_practice_name,
    latest_completed_date,
    latest_screening_date,
    programme_status,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref(indicator_model) }}
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
