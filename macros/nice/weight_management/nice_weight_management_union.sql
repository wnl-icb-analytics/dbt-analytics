{#-
    Combine the NICE weight management measures with their family detail columns.
    Args: reference is current or by_month.
    Returns: one eligible person and indicator per reporting_date.
-#}
{% macro nice_weight_management_union(reference='current') %}
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
    latest_bmi_date,
    bmi_value,
    is_recorded_white,
    bmi_lower_threshold,
    bmi_upper_threshold,
    latest_weight_advice_date,
    latest_record_date,
    is_in_denominator, is_in_numerator, indicator_status,
    NULL::VARCHAR AS bmi_source,
    NULL::BOOLEAN AS requires_lower_bmi_thresholds,
    NULL::VARCHAR AS bmi_category,
    NULL::DATE AS timely_referral_date,
    NULL::DATE AS timely_offer_date,
    NULL::DATE AS timely_decline_date,
    NULL::DATE AS latest_referral_date,
    NULL::DATE AS latest_attendance_date,
    NULL::DATE AS latest_end_date,
    NULL::BOOLEAN AS has_previous_referral,
    NULL::BOOLEAN AS is_currently_attending,
    NULL::BOOLEAN AS has_hypertension,
    NULL::BOOLEAN AS has_diabetes
FROM {{ ref('fct_person_weight_management_advice_overweight_ind319' if reference == 'current' else 'fct_person_weight_management_advice_overweight_ind319_by_month') }}

UNION ALL

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
    latest_bmi_date,
    NULL::FLOAT AS bmi_value,
    NULL::BOOLEAN AS is_recorded_white,
    NULL::FLOAT AS bmi_lower_threshold,
    NULL::FLOAT AS bmi_upper_threshold,
    NULL::DATE AS latest_weight_advice_date,
    latest_record_date,
    is_in_denominator, is_in_numerator, indicator_status,
    NULL::VARCHAR AS bmi_source,
    NULL::BOOLEAN AS requires_lower_bmi_thresholds,
    NULL::VARCHAR AS bmi_category,
    NULL::DATE AS timely_referral_date,
    NULL::DATE AS timely_offer_date,
    NULL::DATE AS timely_decline_date,
    NULL::DATE AS latest_referral_date,
    NULL::DATE AS latest_attendance_date,
    NULL::DATE AS latest_end_date,
    NULL::BOOLEAN AS has_previous_referral,
    NULL::BOOLEAN AS is_currently_attending,
    NULL::BOOLEAN AS has_hypertension,
    NULL::BOOLEAN AS has_diabetes
FROM {{ ref('fct_person_bmi_recording_ind320' if reference == 'current' else 'fct_person_bmi_recording_ind320_by_month') }}
UNION ALL
SELECT person_id, indicator_id, indicator_name, indicator_description, reporting_date, measurement_period_start,
    age, denominator_description, {{ nice_practice_columns(none, reference) }},
    latest_bmi_date, bmi_value, NULL::BOOLEAN AS is_recorded_white,
    NULL::FLOAT AS bmi_lower_threshold, NULL::FLOAT AS bmi_upper_threshold,
    NULL::DATE AS latest_weight_advice_date, latest_record_date,
    is_in_denominator, is_in_numerator, indicator_status,
    bmi_source,
    requires_lower_bmi_thresholds,
    bmi_category,
    timely_referral_date,
    timely_offer_date,
    timely_decline_date,
    latest_referral_date,
    latest_attendance_date,
    latest_end_date,
    has_previous_referral,
    is_currently_attending,
    NULL::BOOLEAN AS has_hypertension,
    NULL::BOOLEAN AS has_diabetes
FROM {{ ref('fct_person_weight_management_offer_obesity_ind220' if reference == 'current' else 'fct_person_weight_management_offer_obesity_ind220_by_month') }}

UNION ALL
SELECT person_id, indicator_id, indicator_name, indicator_description, reporting_date, measurement_period_start,
    age, denominator_description, {{ nice_practice_columns(none, reference) }},
    latest_bmi_date, bmi_value, NULL::BOOLEAN AS is_recorded_white,
    NULL::FLOAT AS bmi_lower_threshold, NULL::FLOAT AS bmi_upper_threshold,
    NULL::DATE AS latest_weight_advice_date, latest_record_date,
    is_in_denominator, is_in_numerator, indicator_status,
    bmi_source,
    requires_lower_bmi_thresholds,
    bmi_category,
    timely_referral_date,
    timely_offer_date,
    timely_decline_date,
    latest_referral_date,
    latest_attendance_date,
    latest_end_date,
    has_previous_referral,
    is_currently_attending,
    has_hypertension,
    has_diabetes
FROM {{ ref('fct_person_weight_management_referral_obesity_htn_dm_ind221' if reference == 'current' else 'fct_person_weight_management_referral_obesity_htn_dm_ind221_by_month') }}
{% endmacro %}
