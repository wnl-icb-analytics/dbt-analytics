{{ config(materialized='table') }}

-- Common long-form interface for the NICE multiple long-term conditions indicator views.
{% set indicator_models = [
    'fct_person_multimorbidity_ind207',
    'fct_person_multimorbidity_ind208'
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
    ltc_count,
    multimorbidity_cluster_count,
    latest_frailty_severity,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref(indicator_model) }}
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
