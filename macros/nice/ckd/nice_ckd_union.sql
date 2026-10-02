{% macro nice_ckd_union(reference='current') %}
{#-
    Combine the nine kidney indicators with their existing detail projection.
    Args: reference is current or by_month.
    Returns: one person/indicator per reporting_date.
-#}
{% if reference not in ['current', 'by_month'] %}
    {{ exceptions.raise_compiler_error('Unsupported NICE reference: ' ~ reference) }}
{% endif %}
-- Common long-form interface for the NICE chronic kidney disease indicator views.
-- Detail columns a view does not emit are typed nulls for its branch.
SELECT
    person_id,
    indicator_id,
    indicator_name,
    reporting_date,
    measurement_period_start,
    age,
    condition_name,
    {{ nice_practice_columns(none, reference) }},
    NULL::FLOAT AS latest_egfr_value,
    NULL::FLOAT AS latest_acr_value,
    NULL::NUMBER AS latest_systolic_value,
    NULL::NUMBER AS latest_diastolic_value,
    NULL::VARCHAR AS latest_frailty_severity,
    NULL::DATE AS latest_therapy_order_date,
    NULL::DATE AS diagnosis_date,
    NULL::DATE AS latest_bp_date,
    NULL::VARCHAR AS applied_measurement_context,
    NULL::NUMBER AS indicator_systolic_threshold,
    NULL::NUMBER AS indicator_diastolic_threshold,
    NULL::NUMBER AS nsaid_issue_day_count,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_ckd_albumin_testing_ind144' if reference == 'current' else 'fct_person_ckd_albumin_testing_ind144_by_month') }}

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
    latest_egfr_value,
    latest_acr_value,
    NULL::NUMBER AS latest_systolic_value,
    NULL::NUMBER AS latest_diastolic_value,
    NULL::VARCHAR AS latest_frailty_severity,
    latest_therapy_order_date,
    NULL::DATE AS diagnosis_date,
    NULL::DATE AS latest_bp_date,
    NULL::VARCHAR AS applied_measurement_context,
    NULL::NUMBER AS indicator_systolic_threshold,
    NULL::NUMBER AS indicator_diastolic_threshold,
    NULL::NUMBER AS nsaid_issue_day_count,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_ckd_ras_therapy_ind263' if reference == 'current' else 'fct_person_ckd_ras_therapy_ind263_by_month') }}

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
    latest_egfr_value,
    latest_acr_value,
    NULL::NUMBER AS latest_systolic_value,
    NULL::NUMBER AS latest_diastolic_value,
    NULL::VARCHAR AS latest_frailty_severity,
    latest_therapy_order_date,
    NULL::DATE AS diagnosis_date,
    NULL::DATE AS latest_bp_date,
    NULL::VARCHAR AS applied_measurement_context,
    NULL::NUMBER AS indicator_systolic_threshold,
    NULL::NUMBER AS indicator_diastolic_threshold,
    NULL::NUMBER AS nsaid_issue_day_count,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_ckd_ras_therapy_ind130' if reference == 'current' else 'fct_person_ckd_ras_therapy_ind130_by_month') }}

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
    latest_egfr_value,
    latest_acr_value,
    NULL::NUMBER AS latest_systolic_value,
    NULL::NUMBER AS latest_diastolic_value,
    NULL::VARCHAR AS latest_frailty_severity,
    latest_therapy_order_date,
    NULL::DATE AS diagnosis_date,
    NULL::DATE AS latest_bp_date,
    NULL::VARCHAR AS applied_measurement_context,
    NULL::NUMBER AS indicator_systolic_threshold,
    NULL::NUMBER AS indicator_diastolic_threshold,
    NULL::NUMBER AS nsaid_issue_day_count,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_ckd_sglt2_therapy_ind324' if reference == 'current' else 'fct_person_ckd_sglt2_therapy_ind324_by_month') }}

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
    NULL::FLOAT AS latest_egfr_value,
    latest_acr_value,
    latest_systolic_value,
    latest_diastolic_value,
    latest_frailty_severity,
    NULL::DATE AS latest_therapy_order_date,
    NULL::DATE AS diagnosis_date,
    latest_bp_date,
    applied_measurement_context,
    indicator_systolic_threshold,
    indicator_diastolic_threshold,
    NULL::NUMBER AS nsaid_issue_day_count,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_ckd_bp_ind264' if reference == 'current' else 'fct_person_ckd_bp_ind264_by_month') }}

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
    NULL::FLOAT AS latest_egfr_value,
    latest_acr_value,
    latest_systolic_value,
    latest_diastolic_value,
    latest_frailty_severity,
    NULL::DATE AS latest_therapy_order_date,
    NULL::DATE AS diagnosis_date,
    latest_bp_date,
    applied_measurement_context,
    indicator_systolic_threshold,
    indicator_diastolic_threshold,
    NULL::NUMBER AS nsaid_issue_day_count,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_ckd_bp_ind235' if reference == 'current' else 'fct_person_ckd_bp_ind235_by_month') }}

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
    latest_egfr_value,
    latest_acr_value,
    NULL::NUMBER AS latest_systolic_value,
    NULL::NUMBER AS latest_diastolic_value,
    NULL::VARCHAR AS latest_frailty_severity,
    NULL::DATE AS latest_therapy_order_date,
    diagnosis_date,
    NULL::DATE AS latest_bp_date,
    NULL::VARCHAR AS applied_measurement_context,
    NULL::NUMBER AS indicator_systolic_threshold,
    NULL::NUMBER AS indicator_diastolic_threshold,
    NULL::NUMBER AS nsaid_issue_day_count,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_ckd_new_diagnosis_egfr_ind233' if reference == 'current' else 'fct_person_ckd_new_diagnosis_egfr_ind233_by_month') }}

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
    latest_egfr_value,
    latest_acr_value,
    NULL::NUMBER AS latest_systolic_value,
    NULL::NUMBER AS latest_diastolic_value,
    NULL::VARCHAR AS latest_frailty_severity,
    NULL::DATE AS latest_therapy_order_date,
    diagnosis_date,
    NULL::DATE AS latest_bp_date,
    NULL::VARCHAR AS applied_measurement_context,
    NULL::NUMBER AS indicator_systolic_threshold,
    NULL::NUMBER AS indicator_diastolic_threshold,
    NULL::NUMBER AS nsaid_issue_day_count,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_ckd_new_diagnosis_tests_ind234' if reference == 'current' else 'fct_person_ckd_new_diagnosis_tests_ind234_by_month') }}

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
    NULL::FLOAT AS latest_egfr_value,
    NULL::FLOAT AS latest_acr_value,
    NULL::NUMBER AS latest_systolic_value,
    NULL::NUMBER AS latest_diastolic_value,
    NULL::VARCHAR AS latest_frailty_severity,
    latest_therapy_order_date,
    NULL::DATE AS diagnosis_date,
    NULL::DATE AS latest_bp_date,
    NULL::VARCHAR AS applied_measurement_context,
    NULL::NUMBER AS indicator_systolic_threshold,
    NULL::NUMBER AS indicator_diastolic_threshold,
    nsaid_issue_day_count,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_nsaid_egfr_ind232' if reference == 'current' else 'fct_person_nsaid_egfr_ind232_by_month') }}


{% endmacro %}
