{% macro nice_ltc_reviews_union(reference='current') %}
{#-
    Combine the explicit NICE LTC review members at their shared detail grain.
    Args: reference is current or by_month; every referenced member must exist.
    Returns: the LTC review family columns, one person/indicator per reporting_date.
-#}
{% if reference not in ['current', 'by_month'] %}
    {{ exceptions.raise_compiler_error('Unsupported NICE reference: ' ~ reference) }}
{% endif %}
-- Common long-form interface for the NICE long-term condition review indicator views.
-- Detail columns a view does not emit are typed nulls for its branch.
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
    latest_review_date,
    NULL::DATE AS latest_copd_exacerbation_count_date,
    NULL::DATE AS latest_mrc_dyspnoea_date,
    NULL::DATE AS latest_nyha_date,
    NULL::DATE AS latest_medication_review_date,
    NULL::DATE AS latest_health_action_plan_date,
    NULL::BOOLEAN AS has_ethnicity_recorded,
    NULL::DATE AS diagnosis_date,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status,
    NULL::DATE AS first_invitation_date,
    NULL::DATE AS last_invitation_date,
    NULL::BOOLEAN AS is_excluded_invitation_non_response
FROM {{ ref('fct_person_asthma_review_ind273' if reference == 'current' else 'fct_person_asthma_review_ind273' ~ '_by_month') }}

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
    latest_review_date,
    latest_copd_exacerbation_count_date,
    latest_mrc_dyspnoea_date,
    NULL::DATE AS latest_nyha_date,
    NULL::DATE AS latest_medication_review_date,
    NULL::DATE AS latest_health_action_plan_date,
    NULL::BOOLEAN AS has_ethnicity_recorded,
    NULL::DATE AS diagnosis_date,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status,
    NULL::DATE AS first_invitation_date,
    NULL::DATE AS last_invitation_date,
    NULL::BOOLEAN AS is_excluded_invitation_non_response
FROM {{ ref('fct_person_copd_review_ind191' if reference == 'current' else 'fct_person_copd_review_ind191' ~ '_by_month') }}

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
    latest_review_date,
    NULL::DATE AS latest_copd_exacerbation_count_date,
    NULL::DATE AS latest_mrc_dyspnoea_date,
    latest_nyha_date,
    latest_medication_review_date,
    NULL::DATE AS latest_health_action_plan_date,
    NULL::BOOLEAN AS has_ethnicity_recorded,
    NULL::DATE AS diagnosis_date,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status,
    NULL::DATE AS first_invitation_date,
    NULL::DATE AS last_invitation_date,
    NULL::BOOLEAN AS is_excluded_invitation_non_response
FROM {{ ref('fct_person_heart_failure_review_ind195' if reference == 'current' else 'fct_person_heart_failure_review_ind195' ~ '_by_month') }}

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
    latest_review_date,
    NULL::DATE AS latest_copd_exacerbation_count_date,
    NULL::DATE AS latest_mrc_dyspnoea_date,
    NULL::DATE AS latest_nyha_date,
    NULL::DATE AS latest_medication_review_date,
    NULL::DATE AS latest_health_action_plan_date,
    NULL::BOOLEAN AS has_ethnicity_recorded,
    NULL::DATE AS diagnosis_date,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status,
    NULL::DATE AS first_invitation_date,
    NULL::DATE AS last_invitation_date,
    NULL::BOOLEAN AS is_excluded_invitation_non_response
FROM {{ ref('fct_person_rheumatoid_arthritis_review_ind110' if reference == 'current' else 'fct_person_rheumatoid_arthritis_review_ind110' ~ '_by_month') }}

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
    latest_review_date,
    NULL::DATE AS latest_copd_exacerbation_count_date,
    NULL::DATE AS latest_mrc_dyspnoea_date,
    NULL::DATE AS latest_nyha_date,
    NULL::DATE AS latest_medication_review_date,
    NULL::DATE AS latest_health_action_plan_date,
    NULL::BOOLEAN AS has_ethnicity_recorded,
    NULL::DATE AS diagnosis_date,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status,
    NULL::DATE AS first_invitation_date,
    NULL::DATE AS last_invitation_date,
    NULL::BOOLEAN AS is_excluded_invitation_non_response
FROM {{ ref('fct_person_hypothyroidism_tft_ind139' if reference == 'current' else 'fct_person_hypothyroidism_tft_ind139' ~ '_by_month') }}

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
    latest_review_date,
    NULL::DATE AS latest_copd_exacerbation_count_date,
    NULL::DATE AS latest_mrc_dyspnoea_date,
    NULL::DATE AS latest_nyha_date,
    NULL::DATE AS latest_medication_review_date,
    latest_health_action_plan_date,
    NULL::BOOLEAN AS has_ethnicity_recorded,
    NULL::DATE AS diagnosis_date,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status,
    NULL::DATE AS first_invitation_date,
    NULL::DATE AS last_invitation_date,
    NULL::BOOLEAN AS is_excluded_invitation_non_response
FROM {{ ref('fct_person_learning_disability_health_check_ind265' if reference == 'current' else 'fct_person_learning_disability_health_check_ind265' ~ '_by_month') }}

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
    latest_review_date,
    NULL::DATE AS latest_copd_exacerbation_count_date,
    NULL::DATE AS latest_mrc_dyspnoea_date,
    NULL::DATE AS latest_nyha_date,
    NULL::DATE AS latest_medication_review_date,
    latest_health_action_plan_date,
    has_ethnicity_recorded,
    NULL::DATE AS diagnosis_date,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status,
    NULL::DATE AS first_invitation_date,
    NULL::DATE AS last_invitation_date,
    NULL::BOOLEAN AS is_excluded_invitation_non_response
FROM {{ ref('fct_person_learning_disability_health_check_ind266' if reference == 'current' else 'fct_person_learning_disability_health_check_ind266' ~ '_by_month') }}

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
    latest_review_date,
    NULL::DATE AS latest_copd_exacerbation_count_date,
    NULL::DATE AS latest_mrc_dyspnoea_date,
    NULL::DATE AS latest_nyha_date,
    NULL::DATE AS latest_medication_review_date,
    NULL::DATE AS latest_health_action_plan_date,
    NULL::BOOLEAN AS has_ethnicity_recorded,
    diagnosis_date,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status,
    NULL::DATE AS first_invitation_date,
    NULL::DATE AS last_invitation_date,
    NULL::BOOLEAN AS is_excluded_invitation_non_response
FROM {{ ref('fct_person_cancer_care_review_ind223' if reference == 'current' else 'fct_person_cancer_care_review_ind223' ~ '_by_month') }}

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
    latest_review_date,
    NULL::DATE AS latest_copd_exacerbation_count_date,
    NULL::DATE AS latest_mrc_dyspnoea_date,
    NULL::DATE AS latest_nyha_date,
    NULL::DATE AS latest_medication_review_date,
    NULL::DATE AS latest_health_action_plan_date,
    NULL::BOOLEAN AS has_ethnicity_recorded,
    NULL::DATE AS diagnosis_date,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status,
    NULL::DATE AS first_invitation_date,
    NULL::DATE AS last_invitation_date,
    NULL::BOOLEAN AS is_excluded_invitation_non_response
FROM {{ ref('fct_person_dementia_care_plan_ind142' if reference == 'current' else 'fct_person_dementia_care_plan_ind142' ~ '_by_month') }}

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
    latest_review_date,
    NULL::DATE AS latest_copd_exacerbation_count_date,
    NULL::DATE AS latest_mrc_dyspnoea_date,
    NULL::DATE AS latest_nyha_date,
    NULL::DATE AS latest_medication_review_date,
    NULL::DATE AS latest_health_action_plan_date,
    NULL::BOOLEAN AS has_ethnicity_recorded,
    diagnosis_date,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status,
    NULL::DATE AS first_invitation_date,
    NULL::DATE AS last_invitation_date,
    NULL::BOOLEAN AS is_excluded_invitation_non_response
FROM {{ ref('fct_person_depression_review_ind104' if reference == 'current' else 'fct_person_depression_review_ind104' ~ '_by_month') }}

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
    latest_thyroid_function_test_date AS latest_review_date,
    NULL::DATE AS latest_copd_exacerbation_count_date,
    NULL::DATE AS latest_mrc_dyspnoea_date,
    NULL::DATE AS latest_nyha_date,
    NULL::DATE AS latest_medication_review_date,
    NULL::DATE AS latest_health_action_plan_date,
    NULL::BOOLEAN AS has_ethnicity_recorded,
    downs_syndrome_diagnosis_date AS diagnosis_date,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status,
    NULL::DATE AS first_invitation_date,
    NULL::DATE AS last_invitation_date,
    NULL::BOOLEAN AS is_excluded_invitation_non_response
FROM {{ ref('fct_person_learning_disability_thyroid_test_ind79' if reference == 'current' else 'fct_person_learning_disability_thyroid_test_ind79' ~ '_by_month') }}

UNION ALL

SELECT
    person_id, indicator_id, indicator_name, indicator_description, reporting_date, measurement_period_start,
    age, denominator_description,
    {{ nice_practice_columns(none, reference) }},
    latest_review_date,
    NULL::DATE AS latest_copd_exacerbation_count_date,
    NULL::DATE AS latest_mrc_dyspnoea_date,
    NULL::DATE AS latest_nyha_date,
    NULL::DATE AS latest_medication_review_date,
    NULL::DATE AS latest_health_action_plan_date,
    NULL::BOOLEAN AS has_ethnicity_recorded,
    diagnosis_date,
    latest_record_date, is_in_denominator, is_in_numerator, indicator_status,
    first_invitation_date, last_invitation_date, is_excluded_invitation_non_response
FROM {{ ref('fct_person_cancer_care_review_ind113' if reference == 'current' else 'fct_person_cancer_care_review_ind113_by_month') }}
UNION ALL
SELECT person_id, indicator_id, indicator_name, indicator_description, reporting_date, measurement_period_start,
    age, denominator_description,
    {{ nice_practice_columns(none, reference) }},
    latest_record_date AS latest_review_date,
    NULL::DATE AS latest_copd_exacerbation_count_date,
    NULL::DATE AS latest_mrc_dyspnoea_date,
    NULL::DATE AS latest_nyha_date,
    NULL::DATE AS latest_medication_review_date,
    NULL::DATE AS latest_health_action_plan_date,
    NULL::BOOLEAN AS has_ethnicity_recorded,
    diagnosis_date, latest_record_date, is_in_denominator, is_in_numerator, indicator_status,
    NULL::DATE AS first_invitation_date, NULL::DATE AS last_invitation_date,
    NULL::BOOLEAN AS is_excluded_invitation_non_response
FROM {{ ref('fct_person_cancer_support_ind222' if reference == 'current' else 'fct_person_cancer_support_ind222_by_month') }}
{% endmacro %}
