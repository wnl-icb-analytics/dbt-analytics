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
    , end_date
    , end_time
    , duration
    , duration_to_date
    , spell_admission_method
    , admission_method_name
    , admission_method_group
    , admission_patient_classification
    , pod
    , primary_diagnosis_code
    , secondary_diagnosis_code
    , primary_treatment
    , main_specialty_code
    , main_specialty_name
    , main_specialty_category
    , treatment_function_code
    , treatment_function_code_desc
    , hrg_code
    , core_hrg_desc
    , core_hrg_chapter
    , core_hrg_chapter_desc
    , spec_comm_flag
    , spec_comm
    , type
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
    , discharge_destination_code
    , discharge_destination_name
    , discharge_method_code
    , discharge_method_name
    , critical_care_days_for_length_of_stay
from {{ ref('int_sus_apc_encounter') }}
