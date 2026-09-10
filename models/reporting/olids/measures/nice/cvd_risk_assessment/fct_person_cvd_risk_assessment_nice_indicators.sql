{{ config(materialized='table') }}

-- Common long-form interface for the NICE CVD risk assessment indicator views.
{% set indicator_models = [
    'fct_person_cvd_risk_assessment_ind269',
    'fct_person_cvd_risk_assessment_ind270',
    'fct_person_cvd_risk_assessment_ind181',
    'fct_person_cvd_risk_assessment_ind161'
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
    latest_risk_score,
    latest_risk_score_date,
    latest_risk_assessment_date,
    {% if indicator_model == 'fct_person_cvd_risk_assessment_ind161' %}new_diagnosis_date{% else %}NULL AS new_diagnosis_date{% endif %},
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref(indicator_model) }}
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
