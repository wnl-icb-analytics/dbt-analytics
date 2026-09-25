{{ config(materialized='table') }}

select
    g.cyp002_unique_id
    , g.person_id
    , g.unique_csds_id_patient
    , g.local_patient_identifier_extended
    , g.general_medical_practice_code_patient_registration
    , g.start_date_gmp_patient_registration
    , g.end_date_gmp_patient_registration
    , g.organisation_code_gp_practice_responsibility
    , g.gp_distance_from_home
    , g.organisation_code_ccg_of_gp_practice
    , g.organisation_identifier_sub_icb_location_of_gp_practice
    , g.organisation_identifier_icb_of_gp_practice
    , g.dm_icb_registration_submitted
    , g.dm_sub_icb_registration_submitted
    , g.dm_commissioner_derivation_reason
    , g.record_number
    , g.record_start_date
    , g.record_end_date
    , g.unique_submission_id
    , g.organisation_code_provider
    , g.organisation_identifier_code_of_provider
    , g.effective_from
    , g.reporting_period_start_date
    , g.reporting_period_end_date
    , g.file_type
    , h.csds_version
from {{ ref('raw_csds_cyp002gp') }} as g
inner join {{ ref('stg_csds_activesubmission') }} as h
    on g.unique_submission_id = h.unique_submission_id
