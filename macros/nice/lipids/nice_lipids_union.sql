{% macro nice_lipids_union(reference='current') %}
{#- Combine the eight LLT measures at person/indicator/date grain. -#}
{% if reference not in ['current', 'by_month'] %}
    {{ exceptions.raise_compiler_error('Unsupported NICE reference: ' ~ reference) }}
{% endif %}
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
    indicator_description,
    reporting_date,
    measurement_period_start,
    age,
    denominator_description,
    {{ nice_practice_columns(none, reference) }},
    latest_lipid_lowering_order_date,
    latest_lipid_lowering_class,
    latest_lipid_lowering_product,
    is_latest_lipid_lowering_statin,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref(indicator_model if reference == 'current' else indicator_model ~ '_by_month') }}
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}

{% endmacro %}
