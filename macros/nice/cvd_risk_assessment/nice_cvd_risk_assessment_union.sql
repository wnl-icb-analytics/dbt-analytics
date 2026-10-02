{% macro nice_cvd_risk_assessment_union(reference='current') %}
{#- Combine the explicit family members at person/indicator/reporting-date grain. -#}
-- Common long-form interface for NICE cardiovascular prevention indicators.
{% set indicator_models = [
    'fct_person_cvd_risk_assessment_ind269',
    'fct_person_cvd_risk_assessment_ind270',
    'fct_person_cvd_risk_assessment_ind181',
    'fct_person_cvd_risk_assessment_ind161',
    'fct_person_blood_pressure_cvd_prevention_ind112'
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
    {{ nice_practice_columns(none, reference) }},
    {% if indicator_model == 'fct_person_blood_pressure_cvd_prevention_ind112' %}
    NULL::FLOAT AS latest_risk_score,
    NULL::DATE AS latest_risk_score_date,
    NULL::DATE AS latest_risk_assessment_date,
    {% else %}
    latest_risk_score,
    latest_risk_score_date,
    latest_risk_assessment_date,
    {% endif %}
    {% if indicator_model == 'fct_person_cvd_risk_assessment_ind161' %}new_diagnosis_date{% else %}NULL AS new_diagnosis_date{% endif %},
    {% if indicator_model == 'fct_person_blood_pressure_cvd_prevention_ind112' %}latest_bp_date{% else %}NULL::DATE AS latest_bp_date{% endif %},
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref(indicator_model if reference == 'current' else indicator_model ~ '_by_month') }}
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
{% endmacro %}
