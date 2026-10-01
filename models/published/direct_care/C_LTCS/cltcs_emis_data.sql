{{
    config(
        materialized='table',
        tags=['cltcs_secure_source'])
}}

with in_scope_practice_list as (
    select practice_code, area_code, area_name
    from {{ ref('cltcs_organisations_in_scope') }}
),

emis_data as (
    select
        ed.sk_patient_id as patient_id,
        ip.area_code as area_code,
        ip.area_name as area_name,
        ed.ltc_lcs,
        ed.borough,
        ed.practice_code,
        ed.age,
        ed.date_of_birth,
        ed.gender,
        ed.ethnic_origin,
        ed.townsend_score,
        ed.lower_layer_area_2001,
        ed.middle_layer_area_2001,
        ed.lower_layer_area_2011,
        ed.middle_layer_area_2011,
        ed.record_of_interpreter_information,
        ed.homelessness,
        ed.housebound,
        ed.care_home_resident,
        ed.cardiovascular_disease,
        ed.atrial_fibrillation,
        ed.hypertension,
        ed.hyperlipidaemia,
        ed.diabetes,
        ed.chronic_kidney_disease,
        ed.na_fatty_liver_disease,
        ed.copd,
        ed.asthma,
        ed.serious_mi,
        ed.learning_disability,
        ed.frailty,
        ed.smoking_status_date,
        ed.smoking_status_code,
        ed.smoking_status_value,
        ed.bmi_date,
        ed.bmi_code,
        ed.bmi_value,
        ed.o_e_bp_reading_date,
        ed.o_e_bp_reading_code,
        ed.o_e_bp_reading_value,
        ed.o_e_bp_reading_secondary_value,
        ed.hba1_c_date,
        ed.hba1_c_code,
        ed.hba1_c_value,
        ed.egfr_date,
        ed.egfr_code,
        ed.egfr_value,
        ed.uacr_date,
        ed.uacr_code,
        ed.uacr_value,
        ed.tc_date,
        ed.tc_code,
        ed.tc_value,
        ed.ldl_date,
        ed.ldl_code,
        ed.ldl_value,
        ed.moc_check_test_appointment_on_date,
        ed.moc_follow_up_appointment_on_date,
        ed.rpt_count_count,
        ed.record_source,
        ed.metadata_record_ingestion_timestamp,
        ed.pds_registered_practice_code,
        ed.pds_registered_practice_mismatch_flag,
        ed.olids_current_practice_code,
        ed.olids_current_practice_mismatch_flag,
    from {{ ref('cltcs_emis_extract') }} ed
    inner join in_scope_practice_list ip
        on ed.practice_code = ip.practice_code
)

select * from emis_data
