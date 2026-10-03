{% macro nice_contraception_union(reference='current') %}
SELECT person_id, indicator_id, indicator_name, reporting_date, measurement_period_start,
    age, condition_name, {{ nice_practice_columns(none, reference) }},
    latest_record_date, is_in_denominator, is_in_numerator, indicator_status,
    latest_asm_date,
    NULL::DATE AS latest_oral_patch_date,
    NULL::DATE AS latest_ehc_date,
    NULL::DATE AS latest_non_specific_advice_date,
    NULL::DATE AS latest_written_advice_date,
    NULL::DATE AS latest_verbal_advice_date,
    NULL::BOOLEAN AS has_unknown_modality_advice
FROM {{ ref('fct_person_epilepsy_contraception_advice_ind117' if reference == 'current' else 'fct_person_epilepsy_contraception_advice_ind117_by_month') }}

UNION ALL
SELECT person_id, indicator_id, indicator_name, reporting_date, measurement_period_start,
    age, condition_name, {{ nice_practice_columns(none, reference) }},
    latest_record_date, is_in_denominator, is_in_numerator, indicator_status,
    NULL::DATE AS latest_asm_date,
    NULL::DATE AS latest_oral_patch_date,
    NULL::DATE AS latest_ehc_date,
    NULL::DATE AS latest_non_specific_advice_date,
    NULL::DATE AS latest_written_advice_date,
    NULL::DATE AS latest_verbal_advice_date,
    NULL::BOOLEAN AS has_unknown_modality_advice
FROM {{ ref('fct_person_diabetes_contraception_advice_ind116' if reference == 'current' else 'fct_person_diabetes_contraception_advice_ind116_by_month') }}

UNION ALL
SELECT person_id, indicator_id, indicator_name, reporting_date, measurement_period_start,
    age, condition_name, {{ nice_practice_columns(none, reference) }},
    latest_record_date, is_in_denominator, is_in_numerator, indicator_status,
    NULL::DATE AS latest_asm_date,
    NULL::DATE AS latest_oral_patch_date,
    NULL::DATE AS latest_ehc_date,
    NULL::DATE AS latest_non_specific_advice_date,
    NULL::DATE AS latest_written_advice_date,
    NULL::DATE AS latest_verbal_advice_date,
    NULL::BOOLEAN AS has_unknown_modality_advice
FROM {{ ref('fct_person_smi_contraception_advice_ind124' if reference == 'current' else 'fct_person_smi_contraception_advice_ind124_by_month') }}

UNION ALL
SELECT person_id, indicator_id, indicator_name, reporting_date, measurement_period_start,
    age, condition_name, {{ nice_practice_columns(none, reference) }},
    latest_record_date, is_in_denominator, is_in_numerator, indicator_status,
    NULL::DATE AS latest_asm_date,
    latest_oral_patch_date,
    NULL::DATE AS latest_ehc_date,
    NULL::DATE AS latest_non_specific_advice_date,
    NULL::DATE AS latest_written_advice_date,
    NULL::DATE AS latest_verbal_advice_date,
    NULL::BOOLEAN AS has_unknown_modality_advice
FROM {{ ref('fct_person_oral_patch_larc_advice_ind148' if reference == 'current' else 'fct_person_oral_patch_larc_advice_ind148_by_month') }}

UNION ALL
SELECT person_id, indicator_id, indicator_name, reporting_date, measurement_period_start,
    age, condition_name, {{ nice_practice_columns(none, reference) }},
    latest_record_date, is_in_denominator, is_in_numerator, indicator_status,
    NULL::DATE AS latest_asm_date,
    NULL::DATE AS latest_oral_patch_date,
    latest_ehc_date,
    latest_non_specific_advice_date,
    latest_written_advice_date,
    latest_verbal_advice_date,
    has_unknown_modality_advice
FROM {{ ref('fct_person_emergency_contraception_larc_advice_ind149' if reference == 'current' else 'fct_person_emergency_contraception_larc_advice_ind149_by_month') }}
{% endmacro %}
