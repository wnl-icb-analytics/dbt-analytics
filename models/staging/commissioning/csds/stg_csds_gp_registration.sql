{{ config(materialized='table') }}

-- Latest reported CYP002 registration for each person, provider, practice and
-- registration start date, the ETOS derivation key. Practice codes are
-- uppercased before keying: about a quarter of source rows carry lowercase
-- codes, which broke practice joins and split one registration in two.
-- Rows without a person ID remain in stg_csds_gp_registration_history only.
select
    cyp002_unique_id
    , person_id
    , unique_csds_id_patient
    , local_patient_identifier_extended
    , upper(general_medical_practice_code_patient_registration)
        as general_medical_practice_code_patient_registration
    , start_date_gmp_patient_registration
    , end_date_gmp_patient_registration
    , organisation_code_gp_practice_responsibility
    , gp_distance_from_home
    , upper(organisation_code_ccg_of_gp_practice) as organisation_code_ccg_of_gp_practice
    , organisation_identifier_sub_icb_location_of_gp_practice
    , organisation_identifier_icb_of_gp_practice
    , dm_icb_registration_submitted
    , dm_sub_icb_registration_submitted
    , dm_commissioner_derivation_reason
    , record_number
    , record_start_date
    , record_end_date
    , unique_submission_id
    , organisation_code_provider
    , organisation_identifier_code_of_provider
    , effective_from
    , reporting_period_start_date
    , reporting_period_end_date
    , file_type
    , csds_version
from {{ ref('stg_csds_gp_registration_history') }}
where person_id is not null
qualify row_number() over (
    partition by person_id, organisation_code_provider,
        upper(general_medical_practice_code_patient_registration), start_date_gmp_patient_registration
    order by reporting_period_end_date desc nulls last, effective_from desc nulls last,
        unique_submission_id::number desc, cyp002_unique_id::number desc
) = 1
