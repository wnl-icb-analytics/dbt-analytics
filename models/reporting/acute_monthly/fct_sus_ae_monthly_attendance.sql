select
    source_records.sk_encounter_id
    , source_records.dv_extract_type
    , source_records.dv_query_id
    , source_records.dv_fin_year
    , source_records.dv_fin_month
    , source_records.dv_activity_period_date
    , source_records.cds_record_type
    , source_records.generated_record_id
    , source_records.spell_identifier
    , source_records.unique_cds_identifier
    , source_records.em_attendance_number
    , source_records.arrival_date
    , source_records.arrival_time
    , source_records.em_referral_source
    , source_records.em_mode_of_arrival
    , source_records.em_department_type
    , source_records.em_attendance_category
    , source_records.em_incident_location_type
    , source_records.em_patient_group
    , source_records.em_staff_member_code
    , source_records.dv_em_initial_assessment_date
    , source_records.em_initial_assessment_time
    , source_records.em_time_seen_for_treatment
    , source_records.dv_em_date_seen_for_treatment
    , source_records.dv_em_attendance_conclusion_date
    , source_records.em_attendance_conclusion_time
    , source_records.em_attendance_disposal
    , source_records.em_departure_date
    , source_records.em_departure_time
    , source_records.number_diagnosis
    , source_records.number_procedures
    , source_records.sk_patient_id
    , source_records.local_patient_identifier
    , source_records.carer_support_indicator
    , source_records.ethnic_category_code
    , source_records.age_at_cds_activity_date as age_at_event
    , source_records.gender_code
    , source_records.postcode_of_usual_address
    , source_records.dv_lsoa
    , source_records.registered_gmp_code_original_data
    , source_records.gp_practice_code
    , source_records.gp_practice_code_original_data
    , source_records.provider_reference_no
    , source_records.commissioner_reference_no
    , source_records.commissioning_serial_no_agreement_no
    , source_records.nhs_service_agreement_line_no
    , source_records.organisation_code_code_of_provider as organisation_id
    , source_records.provider_site_code
    , source_records.organisation_code_code_of_commissioner as submitted_commissioner_code
    , source_records.organisation_code_pct_of_residence as residence_commissioning_area_code
    , source_records.pct_derived_from_gp_practice as practice_commissioning_area_code
    , source_records.patient_postcode_derived_pct
    , source_records.pct_of_residence_original as submitted_residence_commissioning_area_code
    , source_records.pct_responsible as responsible_commissioning_area_code
    , source_records.dv_ccam_swl
    , source_records.dv_commissioner_code_of_residence
    , source_records.dv_purchaser_id
    , source_records.dv_contract_suffix
    , source_records.dv_cost_method_description
    , source_records.core_hrg
    , source_records.dv_hrg
    , source_records.dv_local_cost_code
    , source_records.dv_reason_for_difference
    , source_records.dv_is_pbr
    , source_records.dv_base_cost
    , source_records.dv_mff_index_applied
    , source_records.tariff_initial_amount_national
    , source_records.tariff_pre_mff_adjusted_national
    , source_records.applied_mff_national
    , source_records.tariff_total_payment_national
    , source_records.pbr_final_tariff
    , source_records.dv_total_cost_inc_mff
    , source_records.em_diagnosis_first
    , source_records.em_diagnosis_second_1
    , source_records.em_diagnosis_second_2
    , source_records.em_diagnosis_second_3
    , source_records.em_diagnosis_second_4
    , source_records.em_diagnosis_second_5
    , source_records.em_diagnosis_second_6
    , source_records.em_diagnosis_second_7
    , source_records.em_diagnosis_second_8
    , source_records.em_diagnosis_second_9
    , source_records.em_diagnosis_second_10
    , source_records.em_diagnosis_second_11
    , source_records.em_diagnosis_second_12
    , source_records.em_investigation_first
    , source_records.em_investigation_second_1
    , source_records.em_investigation_second_2
    , source_records.em_investigation_second_3
    , source_records.em_investigation_second_4
    , source_records.em_investigation_second_5
    , source_records.em_investigation_second_6
    , source_records.em_investigation_second_7
    , source_records.em_investigation_second_8
    , source_records.em_investigation_second_9
    , source_records.em_investigation_second_10
    , source_records.em_investigation_second_11
    , source_records.em_investigation_second_12
    , source_records.em_treatment_first
    , source_records.em_treatment_second_1
    , source_records.em_treatment_second_2
    , source_records.em_treatment_second_3
    , source_records.em_treatment_second_4
    , source_records.em_treatment_second_5
    , source_records.em_treatment_second_6
    , source_records.em_treatment_second_7
    , source_records.em_treatment_second_8
    , source_records.em_treatment_second_9
    , source_records.em_treatment_second_10
    , source_records.em_treatment_second_11
    , source_records.em_treatment_second_12
    , source_records.arrival_date::date as activity_date
    , {{ fin_year_from_date('source_records.arrival_date') }} as financial_year
    , provider.organisation_name as organisation_name
    , commissioner.organisation_name as submitted_commissioner_name
from {{ ref('stg_sus_ae_monthly_encounterdenormalised_daterange') }} as source_records
left join {{ ref('organisation') }} as provider
    on upper(trim(source_records.organisation_code_code_of_provider)) = provider.organisation_code
left join {{ ref('organisation') }} as commissioner
    on upper(trim(source_records.organisation_code_code_of_commissioner)) = commissioner.organisation_code
