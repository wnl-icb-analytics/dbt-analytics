{{
    config(materialized = 'table')
}}

WITH deduplication AS ( -- looks for expected unique columns and finds the maximum submission id for said unique columns
    SELECT
        pseudo_nhs_number, 
        referral_identifier, 
        patient_pathway_identifier,
        activity_treatment_function_code, 
        organisation_identifier_code_of_provider,
        organisation_site_identifier_of_treatment,
        referral_to_treatment_period_start_date,
        CASE
            WHEN DAYOFWEEKISO(week_ending_date) = 7 THEN week_ending_date  -- Already Sunday
            ELSE DATEADD('day', -DAYOFWEEK(week_ending_date), week_ending_date)  -- Move to previous Sunday
        END AS week_ending_date,
        date_and_time_data_set_created,
        MAX(der_row_id) AS max_row_id,
        MAX(der_submission_id) AS max_submission_id
    FROM {{ ref('raw_wl_wl_openpathways_data') }}
    GROUP BY ALL
)
SELECT
    CASE
        WHEN DAYOFWEEKISO(a.week_ending_date) = 7 THEN a.week_ending_date  -- Already Sunday
        ELSE DATEADD('day', -DAYOFWEEK(a.week_ending_date), a.week_ending_date)  -- Move to previous Sunday
    END AS week_ending_date,
    {{ consistent_sk_patient_id_format('a.pseudo_nhs_number') }} as sk_patient_id,
    a.local_patient_identifier,
    a.person_stated_gender_code,
    a.ethnic_category,
    a.der_lsoa2021 as lsoa_2021,
    a.der_age_week_ending_date as age_at_week_ending_date,
    a.der_age_at_referral_to_treatment_period_start_date as age_at_referral_to_treatment_period_start_date,
    a.der_age_band_week_ending_date as age_band_week_ending_date,
    a.der_age_band_at_referral_to_treatment_period_start_date as age_band_at_referral_to_treatment_period_start_date,
    a.organisation_identifier_code_of_commissioner as commissioner_code,
    a.der_practice_code as practice_code,
    a.der_ccg_of_practice as ccg_of_practice,
    a.der_ccg_of_residence as ccg_of_residence,
    a.waiting_list_type,
    a.organisation_identifier_code_of_provider as provider_code,
    a.organisation_site_identifier_of_treatment as provider_site_code,
    a.organisation_identifier_referring_organisation as referring_organisation_code,
    a.referral_request_received_date,
    a.original_referral_request_received_date,
    a.referral_to_treatment_period_start_date,
    a.current_pathway_period_start_date,
    a.referral_identifier,
    a.patient_pathway_identifier,
    a.organisation_code_patient_pathway_identifier_issuer,
    a.source_of_referral,
    a.main_specialty_code,
    a.activity_treatment_function_code as treatment_function_code,
    a.consultant_code,
    a.outpatient_future_appointment_date,
    a.due_date,
    a.outpatient_appointment_date,
    a.date_last_attended,
    a.last_dna_date,
    a.cancellation_date,
    a.outcome_of_attendance_code,
    a.tci_date,
    a.referral_to_treatment_period_status,
    a.decision_to_admit_date,
    a.proposed_procedure_opcs_code,
    a.admission_method_code_hospital_provider_spell as admission_method_code,
    a.priority_type_code,
    a.procedure_priority_code,
    a.diagnostic_priority_code,
    a.date_of_last_priority_review,
    a.last_pas_validation_date,
    a.inclusion_on_cancer_ptl,
    a.dm_icb_commissioner,
    a.dm_sub_icb_commissioner,
    a.intended_management_code,
    a.asa_physical_status_classification_system_code,
    a.transfer_status,
    a.mutual_aid_accepting_provider,
    a.first_activity_date,
    a.first_activity_type,
    a.decision_to_treat_date,
    a.tci_date_provided,
    a.preliminary_screening_and_risk_assessment_date,
    a.action_following_preliminary_screening_and_risk_assessment,
    a.date_and_time_data_set_created,
    a.der_submission_id as submission_id,
    a.der_row_id as row_id,
    1 as open_pathways
FROM {{ ref('raw_wl_wl_openpathways_data') }} a
INNER JOIN deduplication d ON a.pseudo_nhs_number = d.pseudo_nhs_number
    AND a.referral_identifier = d.referral_identifier
    AND a.patient_pathway_identifier = d.patient_pathway_identifier
    AND a.activity_treatment_function_code = d.activity_treatment_function_code
    AND a.organisation_identifier_code_of_provider = d.organisation_identifier_code_of_provider
    AND a.organisation_site_identifier_of_treatment = d.organisation_site_identifier_of_treatment
    AND a.referral_to_treatment_period_start_date = d.referral_to_treatment_period_start_date
    AND CASE
        WHEN DAYOFWEEKISO(a.week_ending_date) = 7 THEN a.week_ending_date  -- Already Sunday
        ELSE DATEADD('day', -DAYOFWEEK(a.week_ending_date), a.week_ending_date)  -- Move to previous Sunday
        END = d.week_ending_date
    AND a.date_AND_time_data_set_created = d.date_and_time_data_set_created
    AND a.der_submission_id = d.max_submission_id -- joins on to largest submission ID where duplicate pathways are detected
    AND a.der_row_id = d.max_row_id
WHERE a.week_ending_date IS NOT NULL
    AND CASE
        WHEN DAYOFWEEKISO(a.week_ending_date) = 7 THEN a.week_ending_date  -- Already Sunday
        ELSE DATEADD('day', -DAYOFWEEK(a.week_ending_date), a.week_ending_date)  -- Move to previous Sunday
        END <= CURRENT_DATE
    AND a.referral_to_treatment_period_end_date IS NULL