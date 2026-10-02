{% macro nice_smi_union(reference='current') %}
{#-
    Combine the fourteen NICE SMI measures with their existing detail columns.
    Args: reference is current or by_month; every member must exist.
    Returns: one person/indicator per reporting_date.
-#}
{% if reference not in ['current', 'by_month'] %}
    {{ exceptions.raise_compiler_error('Unsupported NICE reference: ' ~ reference) }}
{% endif %}
-- Common long-form interface for the NICE severe mental illness indicator views.
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
    latest_blood_pressure_date,
    latest_bmi_date,
    latest_alcohol_record_date,
    latest_lipid_date,
    NULL::DATE AS latest_cholesterol_hdl_ratio_date,
    latest_glucose_or_hba1c_date,
    latest_smoking_status_date,
    checks_met_count,
    NULL::VARCHAR AS latest_smoking_status,
    NULL::DATE AS latest_lithium_level_date,
    NULL::FLOAT AS latest_lithium_level,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_smi_physical_health_ind248' if reference == 'current' else 'fct_person_smi_physical_health_ind248' ~ '_by_month') }}

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
    NULL::DATE AS latest_blood_pressure_date,
    NULL::DATE AS latest_bmi_date,
    latest_alcohol_record_date,
    NULL::DATE AS latest_lipid_date,
    NULL::DATE AS latest_cholesterol_hdl_ratio_date,
    NULL::DATE AS latest_glucose_or_hba1c_date,
    NULL::DATE AS latest_smoking_status_date,
    NULL::INTEGER AS checks_met_count,
    NULL::VARCHAR AS latest_smoking_status,
    NULL::DATE AS latest_lithium_level_date,
    NULL::FLOAT AS latest_lithium_level,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_smi_alcohol_ind82' if reference == 'current' else 'fct_person_smi_alcohol_ind82' ~ '_by_month') }}

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
    NULL::DATE AS latest_blood_pressure_date,
    latest_bmi_date,
    NULL::DATE AS latest_alcohol_record_date,
    NULL::DATE AS latest_lipid_date,
    NULL::DATE AS latest_cholesterol_hdl_ratio_date,
    NULL::DATE AS latest_glucose_or_hba1c_date,
    NULL::DATE AS latest_smoking_status_date,
    NULL::INTEGER AS checks_met_count,
    NULL::VARCHAR AS latest_smoking_status,
    NULL::DATE AS latest_lithium_level_date,
    NULL::FLOAT AS latest_lithium_level,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_smi_bmi_ind83' if reference == 'current' else 'fct_person_smi_bmi_ind83' ~ '_by_month') }}

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
    latest_blood_pressure_date,
    NULL::DATE AS latest_bmi_date,
    NULL::DATE AS latest_alcohol_record_date,
    NULL::DATE AS latest_lipid_date,
    NULL::DATE AS latest_cholesterol_hdl_ratio_date,
    NULL::DATE AS latest_glucose_or_hba1c_date,
    NULL::DATE AS latest_smoking_status_date,
    NULL::INTEGER AS checks_met_count,
    NULL::VARCHAR AS latest_smoking_status,
    NULL::DATE AS latest_lithium_level_date,
    NULL::FLOAT AS latest_lithium_level,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_smi_blood_pressure_ind84' if reference == 'current' else 'fct_person_smi_blood_pressure_ind84' ~ '_by_month') }}

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
    NULL::DATE AS latest_blood_pressure_date,
    NULL::DATE AS latest_bmi_date,
    NULL::DATE AS latest_alcohol_record_date,
    NULL::DATE AS latest_lipid_date,
    latest_cholesterol_hdl_ratio_date,
    NULL::DATE AS latest_glucose_or_hba1c_date,
    NULL::DATE AS latest_smoking_status_date,
    NULL::INTEGER AS checks_met_count,
    NULL::VARCHAR AS latest_smoking_status,
    NULL::DATE AS latest_lithium_level_date,
    NULL::FLOAT AS latest_lithium_level,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_smi_cholesterol_ind158' if reference == 'current' else 'fct_person_smi_cholesterol_ind158' ~ '_by_month') }}

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
    NULL::DATE AS latest_blood_pressure_date,
    NULL::DATE AS latest_bmi_date,
    NULL::DATE AS latest_alcohol_record_date,
    NULL::DATE AS latest_lipid_date,
    NULL::DATE AS latest_cholesterol_hdl_ratio_date,
    latest_glucose_or_hba1c_date,
    NULL::DATE AS latest_smoking_status_date,
    NULL::INTEGER AS checks_met_count,
    NULL::VARCHAR AS latest_smoking_status,
    NULL::DATE AS latest_lithium_level_date,
    NULL::FLOAT AS latest_lithium_level,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_smi_glucose_ind159' if reference == 'current' else 'fct_person_smi_glucose_ind159' ~ '_by_month') }}

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
    NULL::DATE AS latest_blood_pressure_date,
    NULL::DATE AS latest_bmi_date,
    NULL::DATE AS latest_alcohol_record_date,
    NULL::DATE AS latest_lipid_date,
    NULL::DATE AS latest_cholesterol_hdl_ratio_date,
    NULL::DATE AS latest_glucose_or_hba1c_date,
    latest_smoking_status_date,
    NULL::INTEGER AS checks_met_count,
    latest_smoking_status,
    NULL::DATE AS latest_lithium_level_date,
    NULL::FLOAT AS latest_lithium_level,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_smi_smoking_ind154' if reference == 'current' else 'fct_person_smi_smoking_ind154' ~ '_by_month') }}

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
    NULL::DATE AS latest_blood_pressure_date,
    NULL::DATE AS latest_bmi_date,
    NULL::DATE AS latest_alcohol_record_date,
    NULL::DATE AS latest_lipid_date,
    NULL::DATE AS latest_cholesterol_hdl_ratio_date,
    NULL::DATE AS latest_glucose_or_hba1c_date,
    latest_smoking_status_date,
    NULL::INTEGER AS checks_met_count,
    latest_smoking_status,
    NULL::DATE AS latest_lithium_level_date,
    NULL::FLOAT AS latest_lithium_level,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_smi_smoking_support_ind155' if reference == 'current' else 'fct_person_smi_smoking_support_ind155' ~ '_by_month') }}

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
    NULL::DATE AS latest_blood_pressure_date,
    NULL::DATE AS latest_bmi_date,
    NULL::DATE AS latest_alcohol_record_date,
    NULL::DATE AS latest_lipid_date,
    NULL::DATE AS latest_cholesterol_hdl_ratio_date,
    NULL::DATE AS latest_glucose_or_hba1c_date,
    NULL::DATE AS latest_smoking_status_date,
    NULL::INTEGER AS checks_met_count,
    NULL::VARCHAR AS latest_smoking_status,
    NULL::DATE AS latest_lithium_level_date,
    NULL::FLOAT AS latest_lithium_level,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_smi_care_plan_ind143' if reference == 'current' else 'fct_person_smi_care_plan_ind143' ~ '_by_month') }}

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
    NULL::DATE AS latest_blood_pressure_date,
    NULL::DATE AS latest_bmi_date,
    NULL::DATE AS latest_alcohol_record_date,
    NULL::DATE AS latest_lipid_date,
    NULL::DATE AS latest_cholesterol_hdl_ratio_date,
    NULL::DATE AS latest_glucose_or_hba1c_date,
    NULL::DATE AS latest_smoking_status_date,
    NULL::INTEGER AS checks_met_count,
    NULL::VARCHAR AS latest_smoking_status,
    NULL::DATE AS latest_lithium_level_date,
    NULL::FLOAT AS latest_lithium_level,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_smi_cvd_risk_assessment_ind150' if reference == 'current' else 'fct_person_smi_cvd_risk_assessment_ind150' ~ '_by_month') }}

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
    NULL::DATE AS latest_blood_pressure_date,
    NULL::DATE AS latest_bmi_date,
    NULL::DATE AS latest_alcohol_record_date,
    NULL::DATE AS latest_lipid_date,
    NULL::DATE AS latest_cholesterol_hdl_ratio_date,
    NULL::DATE AS latest_glucose_or_hba1c_date,
    NULL::DATE AS latest_smoking_status_date,
    NULL::INTEGER AS checks_met_count,
    NULL::VARCHAR AS latest_smoking_status,
    NULL::DATE AS latest_lithium_level_date,
    NULL::FLOAT AS latest_lithium_level,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_smi_cervical_screening_ind85' if reference == 'current' else 'fct_person_smi_cervical_screening_ind85' ~ '_by_month') }}

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
    NULL::DATE AS latest_blood_pressure_date,
    NULL::DATE AS latest_bmi_date,
    NULL::DATE AS latest_alcohol_record_date,
    NULL::DATE AS latest_lipid_date,
    NULL::DATE AS latest_cholesterol_hdl_ratio_date,
    NULL::DATE AS latest_glucose_or_hba1c_date,
    NULL::DATE AS latest_smoking_status_date,
    NULL::INTEGER AS checks_met_count,
    NULL::VARCHAR AS latest_smoking_status,
    NULL::DATE AS latest_lithium_level_date,
    NULL::FLOAT AS latest_lithium_level,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_smi_cervical_screening_ind213' if reference == 'current' else 'fct_person_smi_cervical_screening_ind213' ~ '_by_month') }}

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
    NULL::DATE AS latest_blood_pressure_date,
    NULL::DATE AS latest_bmi_date,
    NULL::DATE AS latest_alcohol_record_date,
    NULL::DATE AS latest_lipid_date,
    NULL::DATE AS latest_cholesterol_hdl_ratio_date,
    NULL::DATE AS latest_glucose_or_hba1c_date,
    NULL::DATE AS latest_smoking_status_date,
    NULL::INTEGER AS checks_met_count,
    NULL::VARCHAR AS latest_smoking_status,
    NULL::DATE AS latest_lithium_level_date,
    NULL::FLOAT AS latest_lithium_level,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_smi_cervical_screening_ind214' if reference == 'current' else 'fct_person_smi_cervical_screening_ind214' ~ '_by_month') }}

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
    NULL::DATE AS latest_blood_pressure_date,
    NULL::DATE AS latest_bmi_date,
    NULL::DATE AS latest_alcohol_record_date,
    NULL::DATE AS latest_lipid_date,
    NULL::DATE AS latest_cholesterol_hdl_ratio_date,
    NULL::DATE AS latest_glucose_or_hba1c_date,
    NULL::DATE AS latest_smoking_status_date,
    NULL::INTEGER AS checks_met_count,
    NULL::VARCHAR AS latest_smoking_status,
    latest_lithium_level_date,
    latest_lithium_level,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_lithium_monitoring_ind87' if reference == 'current' else 'fct_person_lithium_monitoring_ind87' ~ '_by_month') }}
{% endmacro %}
