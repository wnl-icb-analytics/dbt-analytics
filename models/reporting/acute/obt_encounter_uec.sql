{{
    config(
        materialized='table'
    )
}}

with mental_health_diagnosis_attendances as (
    select
        diagnosis.primarykey_id as visit_occurrence_id
        , max(iff(diagnosis.qualifier = '410605003', 1, 0)) = 0
            as has_suspected_mental_health_diagnosis_only
    from {{ ref('stg_sus_ecds_clinical_diagnoses_snomed') }} as diagnosis
    -- NHSE Supplementary ECDS Analysis spec v3.3, section 7.1.3.
    where diagnosis.code in (
        '52448006', '2776000', '33449004', '72366004', '191736004'
        , '371631005', '197480006', '35489007', '13746004', '58214004'
        , '69322001', '44376007', '397923000', '30077003', '17226007'
        , '50705009', '225624000'
    )
    group by diagnosis.primarykey_id
),

psychiatric_referral_attendances as (
    select distinct visit_occurrence_id
    from {{ ref('int_sus_uec_referred_to_service') }}
    where referred_to_service_ecds_group1 = 'Psychiatric'
),

encounters as (
select
    visit_occurrence_id
    , sk_patient_id
    , source
    , local_patient_identifier
    , cds_unique_identifier
    , provider_reference_number
    , organisation_id
    , organisation_name
    , site_id
    , site_name
    , pod
    , department_type
    , department_type_desc
    , uec_activity_type_code
    , uec_activity_type_desc
    , uec_site_label
    , start_date
    , start_time
    , financial_year
    , financial_month
    , financial_month_name
    , week_end_date
    , end_date
    , end_time
    , duration
    , initial_assessment_date
    , initial_assessment_time
    , initial_assessment_time_since_arrival
    , seen_for_treatment_date
    , seen_for_treatment_time
    , seen_for_treatment_time_since_arrival
    , decided_to_admit_date
    , decided_to_admit_time
    , decided_to_admit_time_since_arrival
    , clinically_ready_to_proceed_at
    , clinically_ready_to_proceed_time_since_arrival
    , expected_treatment_at
    , chief_complaint_code
    , chief_complaint_desc
    , chief_complaint_ecds_group1
    , is_injury_related
    , acuity
    , acuity_desc
    , injury_intent_code
    , injury_intent_desc
    , injury_mechanism_code
    , injury_mechanism_desc
    , place_of_injury_code
    , place_of_injury_desc
    , injury_date
    , injury_time
    , disease_notification_code
    , disease_notification_desc
    , primary_diagnosis_code_snomed
    , primary_diagnosis_desc_snomed
    , primary_diagnosis_code_icd10
    , primary_diagnosis_desc_icd10
    , primary_diagnosis_desc_ecds_group1
    , primary_treatment
    , primary_treatment_desc_snomed
    , primary_treatment_desc_ecds_group1
    , primary_investigation
    , primary_investigation_desc_snomed
    , primary_investigation_desc_ecds_group1
    , arrival_mode_code
    , arrival_mode_desc
    , attendance_category_code
    , attendance_category_desc
    , is_arrival_planned
    , ambulance_incident_number
    , conveying_ambulance_trust_code
    , conveying_ambulance_trust_name
    , ambulance_care_contact_identifier
    , attendance_source_code
    , attendance_source_desc
    , attendance_source_organisation_site_identifier
    , attendance_source_organisation_name
    , discharge_destination_code
    , discharge_destination_desc
    , discharge_status_code
    , discharge_status_desc
    , discharge_follow_up_code
    , discharge_follow_up_desc
    , discharge_information_given_code
    , discharge_information_given_desc
    , decided_to_admit_treatment_function_code
    , decided_to_admit_treatment_function_desc
    , receiving_site_id
    , receiving_organisation_name
    , main_specialty_code
    , main_specialty_name
    , hrg_code
    , core_hrg_desc
    , core_hrg_chapter
    , core_hrg_chapter_desc
    , cost
    , applicable_costing_period
    , is_national_tariff_excluded
    , national_tariff
    , national_tariff_final_price
    , mff_factor
    , mff_adjustment

    , residence_area_code_at_event
    , residence_area_name_at_event
    , assigned_commissioner_code_at_event
    , assigned_commissioner_name_at_event
    , age_at_event
    , patient_type_code_at_event
    , patient_type_desc_at_event
    , gender_at_event
    , gender_desc_at_event
    , ethnicity_at_event
    , ethnicity_desc_at_event
    , postcode_id
    , postcode_district_at_event
    , lsoa_11_at_event
    , lsoa_21_at_event
    , lad_at_event
    , imd_at_event
    , deprivation_decile_at_event
    , reg_practice_at_event
    , reg_practice_name_latest
    , practice.pcn_name as reg_practice_pcn_name_latest
    , practice.neighbourhood_name as reg_practice_neighbourhood_name_latest
    , practice.health_borough_name as reg_practice_health_borough_name_latest
    , referral.visit_occurrence_id is not null as is_mental_health_referral
    , coalesce(
        diagnosis.visit_occurrence_id is not null
        or chief_complaint_code in (
            '248062006', '272022009', '366979004', '6471006'
            , '48694002', '248020004', '7011001', '2073000'
        )
        or injury_intent_code = '276853009'
        , false
    ) as is_mental_health_related_attendance
    , coalesce(
        diagnosis.has_suspected_mental_health_diagnosis_only
        , false
    ) as has_suspected_mental_health_diagnosis_only
    , discharge_destination_dictionary.ecds_group1
        as discharge_destination_ecds_group1
    , case
        when discharge_destination_code in (
            '306706006', '1066361000000104', '1066371000000106'
            , '1066381000000108', '1066391000000105', '1066401000000108'
            , '1874161000000104'
        ) then 'Admitted'
        when discharge_destination_code in (
            '306689006', '306691003', '306694006', '306705005', '50861005'
        ) then 'Non-admitted'
        when discharge_destination_code in (
            '305398007', '1066331000000109', '1066341000000100'
            , '1066351000000102', '19712007', '183919006'
        ) then 'Other'
        else 'Unknown'
        end as discharge_destination_group
    , coalesce(
        coalesce(uec_activity_type_code, department_type)
        in ('01', '02', '03', '04')
        and coalesce(attendance_category_code, '') not in ('04', '4', 'X')
        and coalesce(discharge_status_code, '') <> '63238001'
        , false
    ) as is_unplanned_attendance
    , general_practitioner_code
    , general_practitioner_name
    , visit_occurrence_type
from {{ ref('int_sus_uec_encounter') }} as encounter
left join mental_health_diagnosis_attendances as diagnosis
    using (visit_occurrence_id)
left join psychiatric_referral_attendances as referral
    using (visit_occurrence_id)
left join {{ ref('practice_wnl_all') }} as practice
    on encounter.reg_practice_at_event = practice.practice_code
left join {{ ref('stg_dictionary_ecds_dischargedestination') }}
    as discharge_destination_dictionary
    on encounter.discharge_destination_code = discharge_destination_dictionary.snomed_code
)

select
    *
    , coalesce(
        age_at_event < 18 and is_mental_health_related_attendance
        , false
    ) as is_cyp_mental_health
    , case
        when site_id in ('AD915', 'AD904', 'AD906', 'NLO21', 'AD918', 'RY901')
            and department_type in ('3', '03')
            and discharge_status_code in ('1077031000000103', '1077781000000101')
            then 0
        else 1
        end as attendance_count
    , case
        when duration >= 240 then 'Y'
        when duration < 240 then 'N'
        else 'Unknown'
        end as four_hour_breach_status
    , case
        when is_unplanned_attendance
            and duration > 720
            and end_date is not null
            and end_time is not null
            then 1
        else 0
        end as over_12_hours_count
    , iff(duration > 4320, 1, 0) as over_72_hours_count
    , iff(initial_assessment_time_since_arrival <= 15, 1, 0)
        as assessed_within_15_minutes_count
    , iff(initial_assessment_time_since_arrival > 15, 1, 0)
        as not_assessed_within_15_minutes_count
from encounters
