{#-
    Combine the NICE hypertension measures with their family detail columns.
    Args: reference is current or by_month.
    Returns: one eligible person and indicator per reporting_date.
-#}
{% macro nice_hypertension_union(reference='current') %}
{% set members = [
    ('fct_person_urine_acr_hypertension_ind121', 'latest_acr_date'),
    ('fct_person_home_ambulatory_bp_hypertension_ind115', 'latest_home_ambulatory_bp_date'),
    ('fct_person_ecg_hypertension_ind123', 'latest_ecg_date'),
    ('fct_person_haematuria_test_hypertension_ind122', 'latest_haematuria_test_date')
] %}
{% for model, evidence_column in members %}
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
    diagnosis_date,
    {% if evidence_column == 'latest_acr_date' %}latest_acr_date{% else %}NULL::DATE AS latest_acr_date{% endif %},
    {% if evidence_column == 'latest_home_ambulatory_bp_date' %}latest_home_ambulatory_bp_date{% else %}NULL::DATE AS latest_home_ambulatory_bp_date{% endif %},
    is_in_denominator,
    is_in_numerator,
    indicator_status,
    {% if evidence_column == 'latest_ecg_date' %}latest_ecg_date{% else %}NULL::DATE AS latest_ecg_date{% endif %},
    {% if evidence_column == 'latest_ecg_date' %}has_12lead_ecg{% else %}NULL::BOOLEAN AS has_12lead_ecg{% endif %},
    {% if evidence_column == 'latest_haematuria_test_date' %}latest_haematuria_test_date{% else %}NULL::DATE AS latest_haematuria_test_date{% endif %},
    {% if evidence_column == 'latest_haematuria_test_date' %}has_blood_specific_test{% else %}NULL::BOOLEAN AS has_blood_specific_test{% endif %}
FROM {{ ref(model if reference == 'current' else model ~ '_by_month') }}
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
{% endmacro %}
