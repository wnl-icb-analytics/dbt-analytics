
/*
Emergency care encounters from SUS

Clinical Purpose:
- Establishing demand for emergency care services
- Understanding patient service preference
- Care coordination management across providers

Includes all persons (active, inactive and deceased) and all available activity from April 2018.

One row per attendance. Clinical code arrays are in
int_sus_uec_encounter_clinical_codes; onward referrals, Mental Health Act
legal status and alcohol/drug involvement are in their own one-row-per-record
models (int_sus_uec_referred_to_service, int_sus_uec_mental_health_legal_status,
int_sus_uec_injury_alcohol_drug).
*/
with
attendance_category_codes as (
    select
        attendance_category_code
        , attendance_category_desc
    from {{ ref('stg_ukhfd_emergency_care_attendance_category') }}
    ),
uec_activity_type_codes as (
    select
        uec_activity_type_code
        , uec_activity_type_desc
    from {{ ref('stg_ukhfd_uec_activity_type') }}
    ),
department_type_codes as (
    select
        department_type
        , department_type_desc
    from {{ ref('stg_ukhfd_emergency_care_department_type') }}
    ),
ethnicity_codes as (
    select distinct bk_ethnicity_code, ethnicity_desc
    from {{ref('stg_dictionary_dbo_ethnicity')}}
    where ethnicity_code_type = 'Current' or bk_ethnicity_code = '99'
    ),
gender_codes as (
    select distinct gender_code, gender
    from {{ref('stg_dictionary_dbo_gender')}}
    ),
commissioner_codes as (
    select commissioner_code, commissioner_name
    from {{ ref('stg_dictionary_dbo_commissioner') }}
    qualify row_number() over (
        partition by commissioner_code
        order by coalesce(end_date, '9999-12-31'::date) desc, start_date desc
    ) = 1
    ),
treatment_function_codes as (
    select bk_specialty_code, treatment_function_description
    from {{ ref('stg_dictionary_dbo_specialties') }}
    where is_treatment_function = true
    qualify row_number() over (
        partition by bk_specialty_code
        order by date_updated desc nulls last, date_created desc nulls last
    ) = 1
    ),
first_expected_treatment as (
    select primarykey_id, expected_treatment_at
    from {{ ref('stg_sus_ecds_attendance_expected_treatment_times') }}
    qualify row_number() over (
        partition by primarykey_id
        order by expected_treatment_times_id, rownumber_id
    ) = 1
    ),
lsoa_imd_2025 as (
    select
        bridge.old_lsoa_code
        , case
            when count(*) = count(imd.index_of_multiple_deprivation_decile)
              and count(distinct imd.index_of_multiple_deprivation_decile) = 1
                then min(imd.index_of_multiple_deprivation_decile)
          end as unambiguous_imd_2025_decile
    from {{ ref('stg_ukhfd_old_lsoa_to_new_lsoa_map') }} as bridge
    left join {{ ref('stg_reference_imd2025') }} as imd
        on bridge.new_lsoa_code = imd.lsoa_code_2021
    group by bridge.old_lsoa_code
    )

select
    /* Information needed to derive standard encounter information */
    core.primarykey_id as visit_occurrence_id
    , core.sk_patient_id
    , 'SUS_ECDS' as source
    , core.local_patient_identifier
    , core.system_transaction_cds_unique_identifier as cds_unique_identifier
    , core.commissioning_service_agreement_provider_reference_number as provider_reference_number

    /* Location */
    , core.attendance_location_hes_provider_3 as organisation_id
    , dict_org.organisation_name as organisation_name
    , core.attendance_location_site as site_id
    , dict_site.organisation_name as site_name
    , case
        when core.attendance_location_department_type = '01' then 'AE-T1'
        when core.attendance_location_department_type = '02' then 'AE-Other'
        when core.attendance_location_department_type = '03' then 'UCC'
        when core.attendance_location_department_type = '04' then 'WiC'
        when core.attendance_location_department_type = '05' then 'SDEC'
        else 'Others' end as pod
    , core.attendance_location_department_type as department_type
    , department_type.department_type_desc
    , core.attendance_location_activity_type as uec_activity_type_code
    , uec_activity_type.uec_activity_type_desc
    , uec_site.uec_site_label

    /* Time & date */
    , core.attendance_arrival_date as start_date
    , core.attendance_arrival_time as start_time
    , {{ fin_year_from_date('core.attendance_arrival_date') }} as financial_year
    , {{ fin_month_from_date('core.attendance_arrival_date') }} as financial_month
    , date_details.fiscal_calendar_month_name as financial_month_name
    , date_details.end_of_iso_week_date as week_end_date
    , core.attendance_departure_date as end_date
    , core.attendance_departure_time as end_time
    , core.attendance_departure_time_since_arrival as duration
    , core.attendance_initial_assessment_date as initial_assessment_date
    , core.attendance_initial_assessment_time as initial_assessment_time
    , core.attendance_initial_assessment_time_since_arrival as initial_assessment_time_since_arrival
    , core.attendance_seen_for_treatment_date as seen_for_treatment_date
    , core.attendance_seen_for_treatment_time as seen_for_treatment_time
    , core.attendance_seen_for_treatment_time_since_arrival as seen_for_treatment_time_since_arrival
    , core.attendance_decision_to_admit_date as decided_to_admit_date
    , core.attendance_decision_to_admit_time as decided_to_admit_time
    , core.attendance_decision_to_admit_time_since_arrival as decided_to_admit_time_since_arrival
    , core.attendance_clinically_ready_to_proceed_timestamp as clinically_ready_to_proceed_at
    , core.attendance_clinically_ready_to_proceed_time_since_arrival
        as clinically_ready_to_proceed_time_since_arrival
    , expected_treatment.expected_treatment_at

    /* Clinical information */
    -- complaint information
    , core.clinical_chief_complaint_code as chief_complaint_code
    , dict_complaint.snomed_uk_preferred_term as chief_complaint_desc
    , dict_complaint.ecds_group1 as chief_complaint_ecds_group1
    , core.clinical_chief_complaint_is_injury_related as is_injury_related
    , core.clinical_acuity_code as acuity
    , acuity_concept.preferred_term as acuity_desc

    -- injury information is kept with the complaint fields for maintainability
    , core.clinical_injury_intent_code as injury_intent_code
    , injury_intent.snomed_uk_preferred_term as injury_intent_desc
    , core.clinical_injury_mechanism_code as injury_mechanism_code
    , injury_mechanism.snomed_uk_preferred_term as injury_mechanism_desc
    , core.clinical_injury_place_type as place_of_injury_code
    , place_of_injury.snomed_uk_preferred_term as place_of_injury_desc
    , core.clinical_injury_date as injury_date
    , core.clinical_injury_time as injury_time
    , core.clinical_disease_notification_code as disease_notification_code
    , disease_notification.preferred_term as disease_notification_desc

    -- diagnosis information (primary only; all codes in int_sus_uec_encounter_clinical_codes)
    , diagnosis.code as primary_diagnosis_code_snomed
    , diag_dict.snomed_uk_preferred_term as primary_diagnosis_desc_snomed
    , diag_dict.icd10_mapping as primary_diagnosis_code_icd10
    , diag_dict.icd10_description as primary_diagnosis_desc_icd10
    , diag_dict.ecds_group1 as primary_diagnosis_desc_ecds_group1

    -- treatment
    , treatments.code as primary_treatment
    , treat_dict.snomed_uk_preferred_term as primary_treatment_desc_snomed
    , treat_dict.ecds_group1 as primary_treatment_desc_ecds_group1

    -- investigation
    , investigations.code as primary_investigation
    , inv_dict.snomed_uk_preferred_term as primary_investigation_desc_snomed
    , inv_dict.ecds_group1 as primary_investigation_desc_ecds_group1

    /* Arrival information */
    -- arrival mode and desc
    , core.attendance_arrival_arrival_mode_code as arrival_mode_code
    , dict_arrival.snomed_uk_preferred_term as arrival_mode_desc
    , core.attendance_arrival_attendance_category as attendance_category_code
    , attendance_category.attendance_category_desc
    , core.attendance_arrival_planned as is_arrival_planned
    , core.attendance_arrival_ambulance_incident_number as ambulance_incident_number
    , core.attendance_arrival_conveying_ambulance_trust as conveying_ambulance_trust_code
    , ambulance_trust.organisation_name as conveying_ambulance_trust_name
    , core.attendance_arrival_ambulance_care_contact_identifier
        as ambulance_care_contact_identifier
    , core.attendance_arrival_attendance_source_code as attendance_source_code
    , attendance_source.snomed_uk_preferred_term as attendance_source_desc
    , core.attendance_arrival_attendance_source_organisation
        as attendance_source_organisation_site_identifier
    , coalesce(
        attendance_source_site.organisation_name,
        attendance_source_provider.organisation_name
      ) as attendance_source_organisation_name

    /* Discharge information */
    , core.attendance_discharge_destination_code as discharge_destination_code
    , dict_dist.snomed_uk_preferred_term as discharge_destination_desc
    , core.attendance_discharge_status_code as discharge_status_code
    , discharge_status.snomed_uk_preferred_term as discharge_status_desc
    , core.attendance_discharge_follow_up_code as discharge_follow_up_code
    , discharge_follow_up.snomed_uk_preferred_term as discharge_follow_up_desc
    , core.attendance_discharge_information_given_code as discharge_information_given_code
    , discharge_information.preferred_term as discharge_information_given_desc
    , core.attendance_decision_to_admit_treatment_function_code
        as decided_to_admit_treatment_function_code
    , treatment_function.treatment_function_description
        as decided_to_admit_treatment_function_desc
    , core.attendance_decision_to_admit_receiving_site as receiving_site_id
    , coalesce(
        receiving_site.organisation_name,
        receiving_site_provider.organisation_name
      ) as receiving_organisation_name

    /* Clinician information */
    , '180' as main_specialty_code
    , 'Emergency Medicine' as main_specialty_name

    /* Commissioning information */
    , core.commissioning_grouping_health_resource_group as hrg_code
    , dict_hrg.hrg_description as core_hrg_desc
    , dict_hrg.hrg_chapter_key as core_hrg_chapter
    , dict_hrg.hrg_chapter as core_hrg_chapter_desc
    , core.commissioning_national_pricing_final_price as cost
    , core.commissioning_national_pricing_costing_period as applicable_costing_period
    , core.commissioning_national_pricing_excluded as is_national_tariff_excluded
    , core.commissioning_national_pricing_tariff as national_tariff
    , core.commissioning_national_pricing_final_price as national_tariff_final_price
    , core.commissioning_national_pricing_market_forces_factor as mff_factor
    , core.commissioning_national_pricing_market_forces_adjustment as mff_adjustment

    , core.patient_residence_ccg_from_patient_postcode as residence_area_code_at_event
    , coalesce(
        residence_commissioner_exact.commissioner_name
        , residence_commissioner_fallback.commissioner_name
    ) as residence_area_name_at_event
    , core.commissioning_service_agreement_commissioner as assigned_commissioner_code_at_event
    , coalesce(
        registrant_commissioner_exact.commissioner_name
        , registrant_commissioner_fallback.commissioner_name
    ) as assigned_commissioner_name_at_event

    /* patient information at time of event */
    , core.patient_age_at_arrival as age_at_event
    , core.patient_patient_type as patient_type_code_at_event
    , case
        when core.patient_patient_type = 'ADU' then 'Adult'
        when core.patient_patient_type = 'CHI' then 'Child'
        else core.patient_patient_type
      end as patient_type_desc_at_event
    , core.patient_stated_gender as gender_at_event
    , gen.gender as gender_desc_at_event
    , core.patient_ethnic_category as ethnicity_at_event
    , eth.ethnicity_desc as ethnicity_desc_at_event
    , core.patient_usual_address_postcode_pseudo as postcode_id
    , core.patient_usual_address_postcode_district as postcode_district_at_event
    , core.patient_usual_address_lsoa_11 as lsoa_11_at_event
    , core.patient_usual_address_local_authority_district as lad_at_event
    , core.patient_usual_address_index_of_multiple_deprivation_decile as imd_at_event
    , case
        when lsoa_imd.unambiguous_imd_2025_decile is not null
            then lsoa_imd.unambiguous_imd_2025_decile
        when try_to_number(core.patient_usual_address_index_of_multiple_deprivation_decile)
             between 1 and 10
            then try_to_number(core.patient_usual_address_index_of_multiple_deprivation_decile)
      end as deprivation_decile_at_event
    , core.patient_gp_registration_general_practice as reg_practice_at_event
    , registered_practice.organisation_name as reg_practice_name_latest
    , core.patient_gp_registration_general_practitioner as general_practitioner_code
    , general_practitioner.gp_name as general_practitioner_name
    , 'AE_ATTENDANCE' as visit_occurrence_type

from {{ ref('stg_sus_ecds_emergency_care')}} as core

/* Primary diagnosis */
left join {{ref('stg_sus_ecds_clinical_diagnoses_snomed')}} as diagnosis
    on core.primarykey_id = diagnosis.primarykey_id and diagnosis.is_primary = TRUE

left join {{ref('stg_dictionary_ecds_diagnosis')}} as diag_dict
    on diagnosis.code = diag_dict.snomed_code

/* First investigation code */
left join {{ref('stg_sus_ecds_clinical_investigations_snomed')}} as investigations
    on core.primarykey_id = investigations.primarykey_id
    and investigations.snomed_id = 1

left join
    {{ ref('stg_dictionary_ecds_investigation') }} as inv_dict
    on investigations.code = inv_dict.snomed_code

/* First treatment */
left join {{ref('stg_sus_ecds_clinical_treatments_snomed')}} as treatments
    on core.primarykey_id = treatments.primarykey_id
    and treatments.snomed_id = 1

left join
    {{ ref('stg_dictionary_ecds_treatment') }} as treat_dict
    on treatments.code = treat_dict.snomed_code

left join first_expected_treatment as expected_treatment
    on core.primarykey_id = expected_treatment.primarykey_id

/* UEC site label, effective-dated on arrival date */
left join {{ ref('organisation_uec_site') }} as uec_site
    on core.attendance_location_site = uec_site.site_code
    and core.attendance_location_department_type = uec_site.department_type
    and core.attendance_arrival_date between uec_site.effective_from and uec_site.effective_to

/* context dictionaries  */
left join
    {{ref('stg_dictionary_ecds_arrivalmode')}} as dict_arrival
    on core.attendance_arrival_arrival_mode_code = dict_arrival.snomed_code

left join attendance_category_codes as attendance_category
    on core.attendance_arrival_attendance_category
    = attendance_category.attendance_category_code

left join uec_activity_type_codes as uec_activity_type
    on core.attendance_location_activity_type = uec_activity_type.uec_activity_type_code

left join department_type_codes as department_type
    on core.attendance_location_department_type = department_type.department_type

left join {{ ref('stg_dictionary_dbo_dates') }} as date_details
    on core.attendance_arrival_date = date_details.full_date

left join
    {{ref('stg_dictionary_ecds_dischargedestination')}} as dict_dist
    on core.attendance_discharge_destination_code = dict_dist.snomed_code

left join
    {{ref('stg_dictionary_ecds_chiefcomplaint')}} as dict_complaint
    on core.clinical_chief_complaint_code = dict_complaint.snomed_code

left join {{ ref('stg_dictionary_ecds_injuryintent') }} as injury_intent
    on core.clinical_injury_intent_code = injury_intent.snomed_code

left join {{ ref('stg_dictionary_ecds_injurymechanism') }} as injury_mechanism
    on core.clinical_injury_mechanism_code = injury_mechanism.snomed_code

left join {{ ref('stg_dictionary_ecds_placeofinjury') }} as place_of_injury
    on core.clinical_injury_place_type = place_of_injury.snomed_code

left join {{ ref('stg_dictionary_ecds_attendancesource') }} as attendance_source
    on core.attendance_arrival_attendance_source_code = attendance_source.snomed_code

left join {{ ref('stg_dictionary_ecds_dischargestatus') }} as discharge_status
    on core.attendance_discharge_status_code = discharge_status.snomed_code

left join {{ ref('stg_dictionary_ecds_dischargefollowup') }} as discharge_follow_up
    on core.attendance_discharge_follow_up_code = discharge_follow_up.snomed_code

left join {{ ref('stg_dictionary_snomed_concept') }} as disease_notification
    on core.clinical_disease_notification_code = disease_notification.snomed_code

left join {{ ref('stg_dictionary_snomed_concept') }} as discharge_information
    on core.attendance_discharge_information_given_code = discharge_information.snomed_code

left join {{ ref('stg_dictionary_snomed_concept') }} as acuity_concept
    on core.clinical_acuity_code = acuity_concept.snomed_code

/* Demographic dictionaries */
left join ethnicity_codes as eth
    on core.patient_ethnic_category = eth.bk_ethnicity_code

left join gender_codes as gen
    on core.patient_stated_gender = gen.gender_code

-- NHS provider and site reference models are deduplicated to protect encounter grain.
left join {{ ref('organisation_nhs_site') }} as dict_site on
    core.attendance_location_site = dict_site.organisation_code

left join {{ ref('organisation_nhs_provider') }} as ambulance_trust on
    core.attendance_arrival_conveying_ambulance_trust = ambulance_trust.organisation_code

left join {{ ref('organisation_nhs_site') }} as attendance_source_site on
    core.attendance_arrival_attendance_source_organisation = attendance_source_site.organisation_code

-- A very small number of source values are provider rather than site codes.
left join {{ ref('organisation_nhs_provider') }} as attendance_source_provider on
    case
        when length(core.attendance_arrival_attendance_source_organisation) = 5
          and right(core.attendance_arrival_attendance_source_organisation, 2) = '00'
            then left(core.attendance_arrival_attendance_source_organisation, 3)
        else core.attendance_arrival_attendance_source_organisation
    end
    = attendance_source_provider.organisation_code

left join {{ ref('organisation_nhs_site') }} as receiving_site on
    core.attendance_decision_to_admit_receiving_site = receiving_site.organisation_code

left join {{ ref('organisation_nhs_provider') }} as receiving_site_provider on
    case
        when length(core.attendance_decision_to_admit_receiving_site) = 5
          and right(core.attendance_decision_to_admit_receiving_site, 2) = '00'
            then left(core.attendance_decision_to_admit_receiving_site, 3)
        else core.attendance_decision_to_admit_receiving_site
    end
    = receiving_site_provider.organisation_code

-- provider name
left join {{ ref('organisation_nhs_provider') }} as dict_org on
    core.attendance_location_hes_provider_3 = dict_org.organisation_code

left join
    {{ ref('stg_dictionary_dbo_hrg') }} as dict_hrg
    on core.commissioning_grouping_health_resource_group = dict_hrg.hrg_code

left join commissioner_codes as residence_commissioner_exact
    on core.patient_residence_ccg_from_patient_postcode
    = residence_commissioner_exact.commissioner_code

left join commissioner_codes as residence_commissioner_fallback
    on residence_commissioner_exact.commissioner_code is null
    and length(core.patient_residence_ccg_from_patient_postcode) = 5
    and right(core.patient_residence_ccg_from_patient_postcode, 2) = '00'
    and left(core.patient_residence_ccg_from_patient_postcode, 3)
    = residence_commissioner_fallback.commissioner_code

left join commissioner_codes as registrant_commissioner_exact
    on core.commissioning_service_agreement_commissioner
    = registrant_commissioner_exact.commissioner_code

left join commissioner_codes as registrant_commissioner_fallback
    on registrant_commissioner_exact.commissioner_code is null
    and length(core.commissioning_service_agreement_commissioner) = 5
    and right(core.commissioning_service_agreement_commissioner, 2) = '00'
    and left(core.commissioning_service_agreement_commissioner, 3)
    = registrant_commissioner_fallback.commissioner_code

left join treatment_function_codes as treatment_function
    on core.attendance_decision_to_admit_treatment_function_code = treatment_function.bk_specialty_code

left join {{ ref('stg_dictionary_dbo_gp') }} as general_practitioner
    on core.patient_gp_registration_general_practitioner = general_practitioner.gp_code

left join {{ ref('stg_ukhfd_all_gp_and_gdp_practices') }} as registered_practice
    on nullif(upper(trim(core.patient_gp_registration_general_practice)), '')
    = registered_practice.organisation_code

left join lsoa_imd_2025 as lsoa_imd
    on core.patient_usual_address_lsoa_11 = lsoa_imd.old_lsoa_code
