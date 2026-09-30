{{ config(materialized='table') }}

select
    m.cyp001_unique_id
    , m.person_id
    , m.unique_csds_id_patient
    , m.local_patient_identifier_extended
    , m.organisation_code_local_patient_identifier
    , m.nhs_number_status_indicator_code
    , m.person_stated_gender_code
    , m.ethnic_category
    , m.dmic_ethnic_category_group
    , m.language_code_preferred
    , m.person_relationship_main_carer
    , m.postcode_district
    , m.lower_super_output_area_residence
    , m.lower_super_output_area_residence_2011
    , m.census_year
    , m.local_authority_district_unitary_authority
    , m.electoral_ward_of_usual_address
    , m.county
    , m.organisation_code_residence_responsibility
    , m.organisation_identifier_sub_icb_location_of_residence
    , m.organisation_identifier_icb_of_residence
    , m.dm_icb_residence_submitted
    , m.dm_sub_icb_residence_submitted
    , m.dm_commissioner_derivation_reason
    , m.organisation_code_educational_establishment
    , m.looked_after_child_indicator
    , m.safeguarding_vulnerability_factors_indicator
    , m.constant_supervision_and_care_required_due_to_disability_indicator
    , m.educational_assessment_outcome
    , m.health_visitor_first_antenatal_visit_date
    , m.person_at_risk_of_unexpected_death_indicator
    , m.preferred_death_location_discussed_indicator
    , m.death_location_type_code_preferred
    , m.person_death_date
    , m.death_location_type_code_actual
    , m.death_not_at_preferred_location_reason
    , m.ic_age_of_patient_at_rp_start
    , m.ic_age_of_patient_at_rp_end
    , m.ic_age_at_death
    , m.valid_nhs_number_flag
    , m.valid_postcode_flag
    , m.record_number
    , m.record_start_date
    , m.record_end_date
    , m.unique_submission_id
    , m.organisation_code_provider
    , m.organisation_identifier_code_of_provider
    , m.effective_from
    , m.reporting_period_start_date
    , m.reporting_period_end_date
    , m.file_type
    , h.csds_version
from {{ ref('raw_csds_cyp001mpi') }} as m
inner join {{ ref('stg_csds_activesubmission') }} as h
    on m.unique_submission_id = h.unique_submission_id
