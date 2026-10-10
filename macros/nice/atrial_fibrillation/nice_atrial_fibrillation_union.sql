{% macro nice_atrial_fibrillation_union(reference='current') %}
{#- Combine the explicit family members at person/indicator/reporting-date grain. -#}
-- Common long-form interface for the NICE atrial fibrillation indicator views.
{% set indicator_models = [
    'fct_person_atrial_fibrillation_ind128',
    'fct_person_atrial_fibrillation_ind247',
    'fct_person_atrial_fibrillation_ind127',
    'fct_person_atrial_fibrillation_ind169'
] %}

{% for indicator_model in indicator_models %}
SELECT
    person_id,
    indicator_id,
    indicator_name,
    indicator_description,
    reporting_date,
    measurement_period_start,
    age,
    denominator_description,
    {{ nice_practice_columns(none, reference) }},
    latest_chadsvasc_score,
    latest_chadsvasc_date,
    latest_chads2_score,
    latest_anticoagulant_order_date,
    latest_anticoagulant_type,
    latest_doac_order_date,
    latest_vka_order_date,
    is_doac_ineligible,
    has_doac_exception,
    latest_anticoagulant_review_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref(indicator_model if reference == 'current' else indicator_model ~ '_by_month') }}
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
{% endmacro %}
