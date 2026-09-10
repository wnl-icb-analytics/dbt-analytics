{{ config(materialized='table') }}

-- Common long-form interface for the NICE antiplatelet and anticoagulant indicator views.
{% set indicator_models = [
    'fct_person_antithrombotic_therapy_ind132',
    'fct_person_antithrombotic_therapy_ind133',
    'fct_person_antithrombotic_therapy_ind94'
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
    latest_antiplatelet_order_date,
    latest_anticoagulant_order_date,
    latest_anticoagulant_type,
    is_antiplatelet_in_period,
    is_anticoagulant_in_period,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref(indicator_model) }}
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
