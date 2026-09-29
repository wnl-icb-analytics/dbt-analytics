select
    clinical_record_id as assessment_observation_id
    , source_assessment_id
    , is_assessment_parent_linked
    , is_assessment_parent_person_consistent
    , has_person_identifier_changed
    , accepted_source_record_count
    , person_id
    , sk_patient_id
    , referral_source_record_id
    , care_activity_source_record_id
    , uniq_care_cont_id
    , clinical_record_type as assessment_context
    , clinical_code as assessment_concept_code
    , clinical_description as assessment_concept_description
    , assessment_tool_name
    , clinical_at as assessment_recorded_at
    , clinical_time_precision
    , clinical_time_basis
    , clinical_value as submitted_response
    , clinical_value_description as response_description
    , assessment_score_numeric
    , is_assessment_response_non_score
    , assessment_response_status
    , assessment_definition_version
    , assessment_response_definition_version
    , provider_organisation_code
    , provider_organisation_name
    , care_prof_local_id as assessor_local_id
    , uniq_care_prof_local_id as assessor_id
    , reporting_period_start_date
    , reporting_period_end_date
    , source_table
    , source_row_id
    , uniq_submission_id as submission_id
from {{ ref('fct_mhsds_clinical_record') }}
where clinical_record_type in ('referral_assessment', 'activity_assessment', 'clustering_assessment')
