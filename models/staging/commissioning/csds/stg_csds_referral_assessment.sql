{{ config(materialized='table') }}

select
    r.cyp609_unique_id
    , r.unique_service_request_identifier
    , r.coded_assessment_tool_type_snomed_ct
    , r.person_score
    , r.assessment_tool_completion_date
    , r.person_id
    , r.organisation_code_provider
    , r.organisation_identifier_code_of_provider
    , r.unique_submission_id
    , r.record_number
    , r.effective_from
    , r.reporting_period_start_date
    , r.reporting_period_end_date
    , r.file_type
    , h.csds_version
from {{ ref('raw_csds_cyp609codedassessmentreferral') }} as r
inner join {{ ref('stg_csds_activesubmission') }} as h
    on r.unique_submission_id = h.unique_submission_id
