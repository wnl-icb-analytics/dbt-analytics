{% set code_sets = [
    ('accommodation_status', 'raw_ukhfd_data_dictionary_mhsds_accommodation_status'),
    ('accommodation_type', 'raw_ukhfd_data_dictionary_mhsds_accommodation_type'),
    ('settled_accommodation', 'raw_ukhfd_data_dictionary_mhsds_settled_accommodation'),
    ('employment_status', 'raw_ukhfd_data_dictionary_mhsds_employment_status'),
    ('employment_status', 'raw_ukhfd_data_dictionary_mhsds_employment_status_general'),
    ('employment_contract_type', 'raw_ukhfd_data_dictionary_mhsds_employment_contract_type'),
    ('weekly_hours_worked', 'raw_ukhfd_data_dictionary_mhsds_weekly_hours_worked'),
    ('disability', 'raw_ukhfd_data_dictionary_mhsds_disability'),
    ('disability_impact', 'raw_ukhfd_data_dictionary_mhsds_disability_impact'),
    ('care_plan_type', 'raw_ukhfd_data_dictionary_mhsds_care_plan_type'),
    ('care_plan_agreed_by', 'raw_ukhfd_data_dictionary_mhsds_care_plan_agreed_by'),
    ('care_plan_content_agreed_by', 'raw_ukhfd_data_dictionary_mhsds_care_plan_content_agreed_by'),
    ('family_care_plan_involvement', 'raw_ukhfd_data_dictionary_mhsds_family_care_plan_involvement'),
    ('family_care_plan_exclusion_reason', 'raw_ukhfd_data_dictionary_mhsds_family_care_plan_exclusion_reason'),
    ('gender_identity', 'raw_ukhfd_data_dictionary_mhsds_gender_identity'),
    ('gender_same_at_birth', 'raw_ukhfd_data_dictionary_mhsds_gender_same_at_birth'),
    ('person_stated_gender', 'stg_ukhfd_data_dictionary_mhsds_person_stated_gender'),
    ('group_session_type', 'raw_ukhfd_data_dictionary_mhsds_group_session_type'),
    ('drop_in_outcome', 'raw_ukhfd_data_dictionary_mhsds_drop_in_outcome'),
    ('indirect_activity_person_consulted', 'raw_ukhfd_data_dictionary_mhsds_indirect_activity_person_consulted'),
    ('community_treatment_order_end_reason', 'raw_ukhfd_data_dictionary_mhsds_community_treatment_order_end_reason'),
    ('restrictive_intervention_reason', 'raw_ukhfd_data_dictionary_mhsds_restrictive_intervention_reason'),
    ('restrictive_intervention_type', 'raw_ukhfd_data_dictionary_mhsds_restrictive_intervention_type'),
    ('post_incident_review_held', 'raw_ukhfd_data_dictionary_mhsds_post_incident_review_held'),
    ('post_incident_review_not_held_reason', 'raw_ukhfd_data_dictionary_mhsds_post_incident_review_not_held_reason'),
    ('restraint_injury', 'raw_ukhfd_data_dictionary_mhsds_restraint_injury'),
    ('leave_of_absence_end_reason', 'raw_ukhfd_data_dictionary_mhsds_leave_of_absence_end_reason'),
    ('absence_without_leave_end_reason', 'raw_ukhfd_data_dictionary_mhsds_absence_without_leave_end_reason'),
    ('escorted_leave', 'raw_ukhfd_data_dictionary_mhsds_escorted_leave'),
    ('discharge_readiness_delay_reason', 'raw_ukhfd_data_dictionary_mhsds_discharge_readiness_delay_reason'),
    ('discharge_readiness_attribution', 'raw_ukhfd_data_dictionary_mhsds_discharge_readiness_attribution'),
    ('waiting_time_measurement_type', 'raw_ukhfd_data_dictionary_mhsds_waiting_time_measurement_type')
] %}

with combined as (
{% for code_set_name, model_name in code_sets %}
select
    '{{ code_set_name }}' as code_set_name,
    '{{ model_name }}' as source_reference_model,
    definitions.*
from (
    {% if model_name.startswith('stg_') %}select * from {{ ref(model_name) }}{% else %}{{ select_ukhfd_data_dictionary_code_set(model_name) }}{% endif %}
) as definitions
{% if not loop.last %}union all by name{% endif %}
{% endfor %}
)

select
    code_set_name,
    source_reference_model,
    source_code_set_name,
    code,
    description,
    short_description,
    category,
    notes,
    valid_from_date,
    valid_to_date,
    is_currently_valid,
    source_unique_key,
    source_imported_at,
    source_created_at,
    is_latest_definition,
    source_effective_from_at,
    source_effective_to_at
from combined
