{% macro nice_hypertension_union(reference='current') %}
{% set members = [
    ('fct_person_urine_acr_hypertension_ind121', 'latest_acr_date'),
    ('fct_person_home_ambulatory_bp_hypertension_ind115', 'latest_home_ambulatory_bp_date')
] %}
{% for model, evidence_column in members %}
SELECT
    person_id,
    indicator_id,
    indicator_name,
    reporting_date,
    measurement_period_start,
    age,
    condition_name,
    {{ nice_practice_columns(none, reference) }},
    diagnosis_date,
    {% if evidence_column == 'latest_acr_date' %}latest_acr_date{% else %}NULL::DATE AS latest_acr_date{% endif %},
    {% if evidence_column == 'latest_home_ambulatory_bp_date' %}latest_home_ambulatory_bp_date{% else %}NULL::DATE AS latest_home_ambulatory_bp_date{% endif %},
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref(model if reference == 'current' else model ~ '_by_month') }}
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
{% endmacro %}
