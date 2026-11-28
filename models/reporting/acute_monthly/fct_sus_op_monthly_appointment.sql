select
    source_records.sk_encounter_id
    , source_records.dv_extract_type
    , source_records.dv_query_id
    , source_records.dv_fin_year
    , source_records.dv_fin_month
    , source_records.dv_activity_period_date
    , source_records.dv_month_at_end_activity
    , source_records.generated_record_id
    , source_records.attendance_identifier
    , source_records.spell_identifier
    , source_records.unique_cds_identifier
    , source_records.referrer_code
    , source_records.referring_organisation_code
    , source_records.referral_request_received_date
    , source_records.priority_type
    , source_records.service_type_requested
    , source_records.source_of_referral_for_outpatients
    , source_records.is_valid_ubrn
    , source_records.ubrn_occurrence_count
    , source_records.organisation_code_patient_pathway_identifier
    , source_records.rtt_patient_pathway_identifier
    , source_records.rtt_period_start_date
    , source_records.rtt_period_end_date
    , source_records.dv_rtt_length_derived
    , source_records.rtt_status
    , source_records.appointment_date
    , source_records.appointment_time
    , source_records.dv_cds_activity_date
    , source_records.administrative_category
    , source_records.first_attendance
    , source_records.attended_or_did_not_attend
    , source_records.outcome_of_attendance
    , source_records.consultant_code
    , source_records.medical_staff_type_seeing_patient
    , source_records.location_type_code
    , source_records.sk_patient_id
    , source_records.local_patient_identifier
    , source_records.ethnic_category_code
    , source_records.age
    , source_records.dv_year_of_birth
    , source_records.gender_code
    , source_records.postcode_of_usual_address
    , source_records.dv_lsoa
    , source_records.gp_code
    , source_records.gp_practice_code_original_data
    , source_records.gp_practice_code_derived
    , source_records.dv_practice_code_validated
    , source_records.provider_reference_no
    , source_records.commissioner_reference_no
    , source_records.commissioning_serial_no_agreement_no
    , source_records.nhs_service_agreement_line_no
    , source_records.main_specialty_code
    , source_records.treatment_function_code
    , source_records.local_sub_specialty_code
    , source_records.clinic_code
    , source_records.dv_spec_com_service_code_national
    , source_records.dv_spec_com_service_code_local
    , source_records.spell_npoc
    , source_records.organisation_code_code_of_provider as organisation_id
    , source_records.site_code_of_treatment
    , source_records.organisation_code_pct_of_residence as residence_commissioning_area_code
    , source_records.patient_postcode_derived_pct
    , source_records.organisation_code_code_of_commissioner as submitted_commissioner_code
    , source_records.pct_derived_from_gp_practice as practice_commissioning_area_code
    , source_records.dv_ccam_swl
    , source_records.dv_commissioner_code_of_residence
    , source_records.dv_purchaser_id
    , source_records.dv_contract_suffix
    , source_records.dv_cost_method_description
    , source_records.core_hrg
    , source_records.dv_hrg_used_for_tariff
    , source_records.dv_hrg
    , source_records.dv_local_cost_code
    , source_records.dv_reason_for_difference
    , source_records.spell_in_pb_r_not_in_pb_r
    , source_records.dv_is_pbr
    , source_records.dv_applicable_tariff
    , source_records.dv_base_cost
    , source_records.dv_tariff_type
    , source_records.dv_mff_index_applied
    , source_records.tariff_initial_amount_national
    , source_records.tariff_pre_mff_adjusted_national
    , source_records.tariff_total_payment_national
    , source_records.pbr_final_tariff
    , source_records.dv_total_cost_inc_mff
    , source_records.number_diagnosis
    , source_records.primary_diagnosis_code
    , source_records.secondary_diagnosis_code_1
    , source_records.secondary_diagnosis_code_2
    , source_records.secondary_diagnosis_code_3
    , source_records.secondary_diagnosis_code_4
    , source_records.secondary_diagnosis_code_5
    , source_records.secondary_diagnosis_code_6
    , source_records.secondary_diagnosis_code_7
    , source_records.secondary_diagnosis_code_8
    , source_records.secondary_diagnosis_code_9
    , source_records.secondary_diagnosis_code_10
    , source_records.secondary_diagnosis_code_11
    , source_records.secondary_diagnosis_code_12
    , source_records.operation_status
    , source_records.number_procedures
    , source_records.primary_procedure_code
    , source_records.primary_procedure_date
    , source_records.secondary_procedure_code_1
    , source_records.secondary_procedure_date_1
    , source_records.secondary_procedure_code_2
    , source_records.secondary_procedure_date_2
    , source_records.secondary_procedure_code_3
    , source_records.secondary_procedure_date_3
    , source_records.secondary_procedure_code_4
    , source_records.secondary_procedure_date_4
    , source_records.secondary_procedure_code_5
    , source_records.secondary_procedure_date_5
    , source_records.secondary_procedure_code_6
    , source_records.secondary_procedure_date_6
    , source_records.secondary_procedure_code_7
    , source_records.secondary_procedure_date_7
    , source_records.secondary_procedure_code_8
    , source_records.secondary_procedure_date_8
    , source_records.secondary_procedure_code_9
    , source_records.secondary_procedure_date_9
    , source_records.secondary_procedure_code_10
    , source_records.secondary_procedure_date_10
    , source_records.secondary_procedure_code_11
    , source_records.secondary_procedure_date_11
    , source_records.secondary_procedure_code_12
    , source_records.secondary_procedure_date_12
    , source_records.dv_unbundled_hrg_1
    , source_records.dv_unbundled_hrg_2
    , source_records.dv_unbundled_hrg_3
    , source_records.dv_unbundled_hrg_4
    , source_records.dv_unbundled_hrg_5
    , source_records.dv_unbundled_hrg_6
    , source_records.dv_unbundled_hrg_7
    , source_records.dv_unbundled_hrg_8
    , source_records.dv_unbundled_hrg_9
    , source_records.dv_unbundled_hrg_10
    , source_records.dv_unbundled_hrg_11
    , source_records.dv_unbundled_hrg_12
    , source_records.dv_number_unbundled_hr_gs
    , source_records.dv_number_unbundled_non_priced_hr_gs
    , source_records.dv_number_unbundled_priced_hr_gs
    , source_records.appointment_date::date as activity_date
    , {{ fin_year_from_date('source_records.appointment_date') }} as financial_year
    , provider.organisation_name as organisation_name
    , commissioner.organisation_name as submitted_commissioner_name
from {{ ref('stg_sus_op_monthly_encounterdenormalised_daterange') }} as source_records
left join {{ ref('organisation') }} as provider
    on upper(trim(source_records.organisation_code_code_of_provider)) = provider.organisation_code
left join {{ ref('organisation') }} as commissioner
    on upper(trim(source_records.organisation_code_code_of_commissioner)) = commissioner.organisation_code
