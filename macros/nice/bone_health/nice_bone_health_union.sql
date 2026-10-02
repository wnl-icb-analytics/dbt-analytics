{% macro nice_bone_health_union(reference='current') %}
SELECT
    person_id,
    indicator_id,
    indicator_name,
    reporting_date,
    measurement_period_start,
    age,
    condition_name,
    {{ nice_practice_columns(none, reference) }},
    latest_bone_sparing_order_date,
    latest_record_date,
    is_in_denominator, is_in_numerator, indicator_status
FROM {{ ref('fct_person_bone_sparing_therapy_osteoporosis_ind91' if reference == 'current' else 'fct_person_bone_sparing_therapy_osteoporosis_ind91_by_month') }}

UNION ALL

SELECT
    person_id,
    indicator_id,
    indicator_name,
    reporting_date,
    measurement_period_start,
    age,
    condition_name,
    {{ nice_practice_columns(none, reference) }},
    latest_bone_sparing_order_date,
    latest_record_date,
    is_in_denominator, is_in_numerator, indicator_status
FROM {{ ref('fct_person_bone_sparing_therapy_fragility_fracture_ind92' if reference == 'current' else 'fct_person_bone_sparing_therapy_fragility_fracture_ind92_by_month') }}
{% endmacro %}
