{{ config(materialized='table') }}

-- Common long-form interface for the NICE lipid-lowering therapy indicator views.
{% set indicator_models = [
    'fct_person_lipid_lowering_therapy_ind230',
    'fct_person_lipid_lowering_therapy_ind231',
    'fct_person_lipid_lowering_therapy_ind276',
    'fct_person_lipid_lowering_therapy_ind277',
    'fct_person_lipid_lowering_therapy_ind229',
    'fct_person_lipid_lowering_therapy_ind274',
    'fct_person_lipid_lowering_therapy_ind275',
    'fct_person_lipid_lowering_therapy_ind287'
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
    latest_lipid_lowering_order_date,
    latest_lipid_lowering_class,
    latest_lipid_lowering_product,
    is_latest_lipid_lowering_statin,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref(indicator_model) }}
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
