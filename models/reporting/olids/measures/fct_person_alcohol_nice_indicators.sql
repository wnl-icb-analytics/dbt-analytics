{{ config(materialized='table') }}

-- Common long-form interface for the NICE alcohol use indicator views.
{% set indicator_models = [
    'fct_person_alcohol_ind196',
    'fct_person_alcohol_ind197',
    'fct_person_alcohol_ind198',
    'fct_person_alcohol_ind199',
    'fct_person_alcohol_ind200',
    'fct_person_alcohol_ind201',
    'fct_person_alcohol_ind202'
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
    {% if indicator_model in ['fct_person_alcohol_ind196', 'fct_person_alcohol_ind197', 'fct_person_alcohol_ind198', 'fct_person_alcohol_ind199'] %}new_diagnosis_date{% else %}CAST(NULL AS DATE) AS new_diagnosis_date{% endif %},
    latest_alcohol_screen_date,
    latest_alcohol_screen_tool,
    latest_alcohol_screen_score,
    latest_positive_alcohol_screen_date,
    latest_intervention_after_positive_screen_date,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref(indicator_model) }}
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
