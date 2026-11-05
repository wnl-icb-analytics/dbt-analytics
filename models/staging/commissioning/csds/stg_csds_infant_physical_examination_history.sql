select
    r.cyp605_unique_id
    , r.local_patient_identifier_extended
    , r.infant_physical_examination_date::date as infant_physical_examination_date
    , r.infant_physical_examination_result_hips as result_hips
    , r.infant_physical_examination_result_heart as result_heart
    , r.infant_physical_examination_result_eyes as result_eyes
    , r.infant_physical_examination_result_testes as result_testes
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
from {{ ref('raw_csds_cyp605ipe') }} as r
inner join {{ ref('stg_csds_activesubmission') }} as h
    on r.unique_submission_id = h.unique_submission_id
