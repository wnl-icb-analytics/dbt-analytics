select
    r.cyp606_unique_id
    , r.service_request_identifier
    , r.unique_service_request_identifier
    , r.diagnosis_scheme_in_use_community_care as diagnosis_scheme_code
    , r.provisional_diagnosis_coded_clinical_entry as diagnosis_code
    , r.provisional_diagnosis_date::date as diagnosis_date
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
from {{ ref('raw_csds_cyp606provdiag') }} as r
inner join {{ ref('stg_csds_activesubmission') }} as h
    on r.unique_submission_id = h.unique_submission_id
