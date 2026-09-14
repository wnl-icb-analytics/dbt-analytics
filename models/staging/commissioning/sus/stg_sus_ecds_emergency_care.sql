{{
    config(materialized = 'view')
}}

-- Deduplicate the lookup before joining so dictionary versions cannot multiply attendances.
with organisation_codes as (
    select distinct organisation_code
    from {{ ref('stg_dictionary_dbo_organisation') }}
    where sk_organisation_type_id = 41
)

select primarykey_id
    , system_transaction_cds_unique_identifier
    , {{ consistent_sk_patient_id_format('patient_nhs_number_value_pseudo') }} as sk_patient_id
    , patient_local_patient_identifier_value as local_patient_identifier
    , commissioning_service_agreement_provider_reference_number
    , commissioning_national_pricing_costing_period
    , commissioning_national_pricing_excluded::boolean
        as commissioning_national_pricing_excluded
    , commissioning_national_pricing_tariff
    , commissioning_national_pricing_market_forces_factor
    , commissioning_national_pricing_market_forces_adjustment
    , patient_patient_type
    , nullif(upper(trim(patient_residence_ccg_from_patient_postcode)), '')
        as patient_residence_ccg_from_patient_postcode
    , nullif(upper(trim(patient_gp_registration_general_practitioner)), '')
        as patient_gp_registration_general_practitioner
    , nullif(upper(trim(commissioning_service_agreement_commissioner)), '')
        as commissioning_service_agreement_commissioner
    , case when provider.organisation_code is not null
        then core.attendance_location_hes_provider_3
        else left(core.attendance_location_hes_provider_3, 3)
      end as attendance_location_hes_provider_3
    , nullif(upper(trim(attendance_location_site)), '') as attendance_location_site
    , attendance_location_department_type
    , attendance_location_activity_type
    , system_record_provider

    -- arrival
    , attendance_arrival_date::date as attendance_arrival_date
    , attendance_arrival_time::time as attendance_arrival_time
    , attendance_arrival_arrival_mode_code
    , attendance_arrival_attendance_category
    , attendance_arrival_planned
    , attendance_arrival_ambulance_incident_number
    , case when ambulance.organisation_code is not null
        then core.attendance_arrival_conveying_ambulance_trust
        else left(core.attendance_arrival_conveying_ambulance_trust, 3)
      end as attendance_arrival_conveying_ambulance_trust
    , attendance_arrival_ambulance_care_contact_identifier
    , attendance_arrival_attendance_source_code
    -- This field predominantly contains five-character site identifiers. Do not
    -- use clean_organisation_id's provider default, which would truncate sites.
    , nullif(upper(trim(attendance_arrival_attendance_source_organisation)), '')
        as attendance_arrival_attendance_source_organisation

    -- assessment and treatment
    , attendance_initial_assessment_date::date as attendance_initial_assessment_date
    , attendance_initial_assessment_time::time as attendance_initial_assessment_time
    , attendance_initial_assessment_time_since_arrival
    , attendance_seen_for_treatment_date::date as attendance_seen_for_treatment_date
    , attendance_seen_for_treatment_time::time as attendance_seen_for_treatment_time
    , attendance_seen_for_treatment_time_since_arrival
    , attendance_conclusion_date::date as attendance_conclusion_date
    , attendance_conclusion_time::time as attendance_conclusion_time
    , attendance_conclusion_time_since_arrival
    , attendance_decision_to_admit_date::date as attendance_decision_to_admit_date
    , attendance_decision_to_admit_time::time as attendance_decision_to_admit_time
    , attendance_decision_to_admit_time_since_arrival
    , attendance_decision_to_admit_treatment_function_code
    -- Receiving-site identifiers are preserved at site grain for dictionary joins.
    , nullif(upper(trim(attendance_decision_to_admit_receiving_site)), '')
        as attendance_decision_to_admit_receiving_site
    , attendance_clinically_ready_to_proceed_timestamp::timestamp_tz
        as attendance_clinically_ready_to_proceed_timestamp
    , attendance_clinically_ready_to_proceed_time_since_arrival

    -- discharge
    , attendance_departure_date::date as attendance_departure_date
    , attendance_departure_time::time as attendance_departure_time
    , attendance_departure_time_since_arrival
    , attendance_discharge_destination_code
    , attendance_discharge_status_code
    , attendance_discharge_follow_up_code
    , attendance_discharge_information_given_code

    -- reasons for attendance
    , clinical_chief_complaint_code
    , clinical_chief_complaint_is_injury_related
    , clinical_acuity_code
    , clinical_injury_intent_code
    , clinical_injury_mechanism_code
    , clinical_injury_place_type
    , nullif(clinical_injury_date::date, '1900-01-01'::date) as clinical_injury_date
    , clinical_injury_time::time as clinical_injury_time
    , clinical_disease_notification_code

    -- cost
    , commissioning_grouping_health_resource_group
    , commissioning_national_pricing_final_price

    -- referral to treatment
    , referral_patient_pathway_identifier_value_pseudo
    , referral_period_status
    , referral_period_start_date::date as referral_period_start_date
    , referral_period_end_date::date as referral_period_end_date
    , referral_waiting_time_measurement_type

    -- patient demographics at time for 2ndry care only analysis
    , patient_age_at_arrival
    , patient_stated_gender
    , patient_ethnic_category
    , patient_usual_address_postcode_pseudo
    , patient_usual_address_postcode_district
    , patient_usual_address_lsoa_11
    , patient_usual_address_local_authority_district
    , patient_usual_address_index_of_multiple_deprivation_decile
    , patient_gp_registration_general_practice

from {{ ref('raw_sus_ecds_emergency_care') }} as core
left join organisation_codes as provider
    on core.attendance_location_hes_provider_3 = provider.organisation_code
left join organisation_codes as ambulance
    on core.attendance_arrival_conveying_ambulance_trust = ambulance.organisation_code
