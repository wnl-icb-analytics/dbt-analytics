{{ config(materialized='table') }}

select
    r.cyp501_unique_id
    , r.local_patient_identifier_extended
    , r.unique_local_patient_id
    , r.immunisation_date
    , r.procedure_scheme_in_use_community_care
    , r.immunisation_procedure_clinical_terminology
    , r.organisation_code_immunisation_responsible_organisation
    , r.organisation_identifier_immunisation_responsible_organisation
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
from {{ ref('raw_csds_cyp501codedimm') }} as r
inner join {{ ref('stg_csds_activesubmission') }} as h
    on r.unique_submission_id = h.unique_submission_id
