{{ config(materialized='table') }}

select
    r.unique_care_activity_identifier
    , r.care_activity_identifier
    , r.unique_care_contact_identifier
    , r.care_contact_identifier
    , r.community_care_activity_type
    , r.care_professional_local_identifier
    , r.unique_care_professional_id_local
    , r.clinical_contact_duration_of_care_activity
    , r.procedure_scheme_in_use_community_care
    , r.coded_procedure_clinical_terminology
    , r.finding_scheme_in_use_community_care
    , r.coded_finding_coded_clinical_entry
    , r.observation_scheme_in_use_community_care
    , r.coded_observation_clinical_terminology
    , r.observation_value
    , r.ucum_unit_of_measurement
    , r.cyp202_unique_id
    , r.person_id
    , r.unique_submission_id
    , r.organisation_code_provider
    , r.organisation_identifier_code_of_provider
    , r.effective_from
    , r.reporting_period_start_date
    , r.reporting_period_end_date
    , r.file_type
    , h.csds_version
from {{ ref('raw_csds_cyp202careactivity') }} as r
inner join {{ ref('stg_csds_activesubmission') }} as h
    on r.unique_submission_id = h.unique_submission_id
