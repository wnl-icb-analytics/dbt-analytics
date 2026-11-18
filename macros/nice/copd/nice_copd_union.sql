{% macro nice_copd_union(reference='current') %}
{#-
    Combine NICE COPD measures with their family detail columns.
    Args: reference is current or by_month.
    Returns: one eligible person and indicator per reporting_date.
-#}
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
    latest_fev1_date,
    NULL::DATE AS latest_fev1_percent_date,
    NULL::FLOAT AS latest_fev1_percent,
    NULL::DATE AS latest_very_severe_code_date,
    NULL::FLOAT AS latest_spo2_value,
    NULL::DATE AS diagnosis_date,
    NULL::DATE AS mrc_date,
    NULL::DATE AS latest_offer_date,
    NULL::DATE AS first_invitation_date,
    NULL::DATE AS last_invitation_date,
    NULL::DATE AS latest_response_date,
    NULL::BOOLEAN AS is_excluded_invitation_non_response,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_copd_fev1_ind140' if reference == 'current' else 'fct_person_copd_fev1_ind140_by_month') }}

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
    NULL::DATE AS latest_fev1_date,
    latest_fev1_percent_date,
    latest_fev1_percent,
    latest_very_severe_code_date,
    latest_spo2_value,
    NULL::DATE AS diagnosis_date,
    NULL::DATE AS mrc_date,
    NULL::DATE AS latest_offer_date,
    NULL::DATE AS first_invitation_date,
    NULL::DATE AS last_invitation_date,
    NULL::DATE AS latest_response_date,
    NULL::BOOLEAN AS is_excluded_invitation_non_response,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_copd_oxygen_saturation_ind212' if reference == 'current' else 'fct_person_copd_oxygen_saturation_ind212_by_month') }}

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
    NULL::DATE AS latest_fev1_date,
    NULL::DATE AS latest_fev1_percent_date,
    NULL::FLOAT AS latest_fev1_percent,
    NULL::DATE AS latest_very_severe_code_date,
    NULL::FLOAT AS latest_spo2_value,
    diagnosis_date,
    mrc_date,
    latest_offer_date,
    first_invitation_date,
    last_invitation_date,
    latest_response_date,
    is_excluded_invitation_non_response,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_copd_pulmonary_rehabilitation_ind101' if reference == 'current' else 'fct_person_copd_pulmonary_rehabilitation_ind101_by_month') }}
{% endmacro %}
