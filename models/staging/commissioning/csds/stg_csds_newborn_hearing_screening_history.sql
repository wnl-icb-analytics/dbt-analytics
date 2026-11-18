select
    r.cyp603_unique_id
    , r.local_patient_identifier_extended
    , r.newborn_hearing_screening_outcome
    , r.service_request_date_newborn_hearing_audiology::date as audiology_service_request_date
    , r.procedure_date_newborn_hearing_audiology::date as audiology_procedure_date
    , r.newborn_hearing_audiology_outcome
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
from {{ ref('raw_csds_cyp603newbornhearingscreening') }} as r
inner join {{ ref('stg_csds_activesubmission') }} as h
    on r.unique_submission_id = h.unique_submission_id
