{{
    config(
        materialized='view'
    )
}}

select
    visit_occurrence_id
    , sk_patient_id
    , source
    , local_patient_identifier
    , organisation_id
    , organisation_name
    , site_id
    , site_name
    , start_date
    , start_time
    , expected_duration
    , appointment_outcome
    , outcome_desc
    , appointment_attended_or_dna
    , appointment_attendance_outcome_desc
    , appointment_first_attendance
    , first_attendance_desc
    , pod
    , primary_diagnosis_code
    , primary_diagnosis_name
    , primary_procedure_code
    , primary_procedure_category
    , primary_procedure_name
    , main_specialty_code
    , main_specialty_name
    , main_specialty_category
    , treatment_function_code
    , treatment_function_code_desc
    , referral_organisation_id
    , referral_organisation_date
    , referral_source_op
    , referral_source_op_desc
    , referral_acuity
    , referral_acuity_desc
    , spec_comm_flag
    , spec_comm_code
    , spec_comm_desc
    , pss_service_line_code
    , pss_service_line_desc
    , core_hrg_code
    , core_hrg_desc
    , core_hrg_chapter
    , core_hrg_chapter_desc
    , cost
    , age_at_event
    , gender_at_event
    , gender_desc_at_event
    , ethnicity_at_event
    , ethnicity_desc_at_event
    , postcode_district_at_event
    , lsoa_11_at_event
    , lad_at_event
    , imd_at_event
    , reg_practice_at_event
    , visit_occurrence_type
from {{ ref('int_sus_op_appointment') }}
