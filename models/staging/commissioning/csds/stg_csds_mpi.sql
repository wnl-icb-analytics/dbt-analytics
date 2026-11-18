{{ config(materialized='table') }}

-- Latest reported CYP001 record for each person and provider, the ETOS
-- derivation key. A provider can submit more than one local patient record
-- for the same person in one month, so the newest source row wins the tie.
-- Rows without a person ID remain in stg_csds_mpi_history only.
select
    cyp001_unique_id
    , person_id
    , unique_csds_id_patient
    , local_patient_identifier_extended
    , organisation_code_local_patient_identifier
    , nhs_number_status_indicator_code
    , person_stated_gender_code
    , ethnic_category
    , dmic_ethnic_category_group
    , language_code_preferred
    , person_relationship_main_carer
    , postcode_district
    , lower_super_output_area_residence
    , lower_super_output_area_residence_2011
    , census_year
    , local_authority_district_unitary_authority
    , electoral_ward_of_usual_address
    , county
    , organisation_code_residence_responsibility
    , organisation_identifier_sub_icb_location_of_residence
    , organisation_identifier_icb_of_residence
    , dm_icb_residence_submitted
    , dm_sub_icb_residence_submitted
    , dm_commissioner_derivation_reason
    , organisation_code_educational_establishment
    , looked_after_child_indicator
    , safeguarding_vulnerability_factors_indicator
    , constant_supervision_and_care_required_due_to_disability_indicator
    , educational_assessment_outcome
    , health_visitor_first_antenatal_visit_date
    , person_at_risk_of_unexpected_death_indicator
    , preferred_death_location_discussed_indicator
    , death_location_type_code_preferred
    , person_death_date
    , death_location_type_code_actual
    , death_not_at_preferred_location_reason
    , ic_age_of_patient_at_rp_start
    , ic_age_of_patient_at_rp_end
    , ic_age_at_death
    , valid_nhs_number_flag
    , valid_postcode_flag
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
from {{ ref('stg_csds_mpi_history') }}
where person_id is not null
qualify row_number() over (
    partition by person_id, organisation_code_provider
    order by reporting_period_end_date desc nulls last, effective_from desc nulls last,
        unique_submission_id::number desc, cyp001_unique_id::number desc
) = 1
