{% macro nice_indicator_status(reference='current') %}
{#-
    Combine the explicit current or monthly NICE status inputs.
    Args: reference is current or by_month; every referenced member must exist.
    Returns: person, indicator, date, age, practice, denominator, numerator and status columns.
-#}
{% if reference not in ['current', 'by_month'] %}
    {{ exceptions.raise_compiler_error('Unsupported NICE reference: ' ~ reference) }}
{% endif %}

-- IND278 and IND267 are standalone inputs, outside the family unions.

{% set indicator_models = [
    'fct_person_bp_control_nice_indicators',
    'fct_person_lipid_lowering_therapy_nice_indicators',
    'fct_person_cholesterol_control_ind278',
    'fct_person_diabetes_nice_indicators',
    'fct_person_antithrombotic_therapy_nice_indicators',
    'fct_person_ckd_nice_indicators',
    'fct_person_cvd_risk_assessment_nice_indicators',
    'fct_person_atrial_fibrillation_nice_indicators',
    'fct_person_hypertension_nice_indicators',
    'fct_person_heart_failure_nice_indicators',
    'fct_person_myocardial_infarction_nice_indicators',
    'fct_person_dementia_nice_indicators',
    'fct_person_asthma_nice_indicators',
    'fct_person_bone_health_nice_indicators',
    'fct_person_colorectal_cancer_fit_ind267',
    'fct_person_smoking_nice_indicators',
    'fct_person_weight_management_nice_indicators',
    'fct_person_multimorbidity_nice_indicators',
    'fct_person_alcohol_nice_indicators',
    'fct_person_cervical_screening_nice_indicators',
    'fct_person_flu_vaccination_nice_indicators',
    'fct_person_shingles_vaccination_ind219',
    'fct_person_childhood_immunisation_nice_indicators',
    'fct_person_ltc_review_nice_indicators',
    'fct_person_smi_nice_indicators'
] %}

{% for indicator_model in indicator_models %}
SELECT
    person_id,
    indicator_id,
    indicator_name,
    reporting_date,
    measurement_period_start,
    age,
    {{ nice_practice_columns(none, reference) }},
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref(indicator_model if reference == 'current' else indicator_model ~ '_by_month') }}
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
{% endmacro %}
