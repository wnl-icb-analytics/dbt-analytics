{{
    config(
        materialized='table',
        tags=['cltcs_secure_source'])
}}

-- Unplanned A&E attendances and emergency admissions for the C-LTCS cohort over the last 12
-- months, with the flags the Cohort Needs Profile page uses to group them by likely service.
-- An attendance that leads to an admission appears once in each setting.
with inclusion_list as (
    select patient_id
        , area_code
    from {{ ref('cltcs_patient_list') }}
),

alcohol_drug_attendances as (
    select distinct visit_occurrence_id
    from {{ ref('int_sus_uec_injury_alcohol_drug') }}
),

ambulatory_sensitive_admissions as (
    select distinct visit_occurrence_id
    from {{ ref('int_activity_ambulatory_sensitive_nel') }}
),

attendances as (
    select *
        , left(replace({{ clean_icd10_code('upper(trim(primary_diagnosis_code_icd10))') }}, '.', ''), 3) as icd10_category
    from {{ ref('obt_encounter_uec') }}
    where is_unplanned_attendance
        and attendance_count = 1
        and start_date between dateadd(month, -12, current_date()) and current_date()
),

-- Same spells as apc_nel_12mo in fct_person_sus_apc_recent. The imputed spells model already
-- excludes births and removes duplicate spells.
admissions as (
    select *
        , replace({{ clean_icd10_code('upper(trim(primary_diagnosis_code))') }}, '.', '') as icd10_code
    from {{ ref('int_sus_apc_imputed_spells') }}
    where left(spell_admission_method, 1) = '2'
        and start_date between dateadd(month, -12, current_date()) and current_date()
),

attendance_events as (
    select il.patient_id
        , il.area_code
        , 'AE' as event_setting
        , a.visit_occurrence_id::varchar as event_id
        , a.start_date as event_date
        , a.start_time as event_time
        , coalesce(a.primary_diagnosis_desc_snomed, a.chief_complaint_desc) as event_reason
        , a.arrival_mode_desc
        , a.attendance_source_desc
        , null::number as length_of_stay_days
        , a.arrival_mode_desc ilike '%ambulance%' as is_ambulance_arrival
        , null::boolean as is_ambulatory_care_sensitive
        , a.discharge_destination_group = 'Admitted' as is_admitted
        , a.is_mental_health_related_attendance as is_mental_health
        , adi.visit_occurrence_id is not null
            or coalesce(a.icd10_category between 'F10' and 'F19', false) as is_alcohol_drug
        , coalesce(
            regexp_like(a.injury_mechanism_desc, '(^|.*[^a-z])falls?([^a-z].*|$)', 'i')
            or regexp_like(a.chief_complaint_desc, '(^|.*[^a-z])falls?([^a-z].*|$)', 'i')
            , false
        ) as is_fall
        , coalesce(
            a.primary_diagnosis_desc_ecds_group1 ilike any (
                '%respiratory%', '%cardiac%', '%cardiovascular%', '%endocrine%', '%metabolic%'
            )
            , false
        ) as is_ltc_flare
        , coalesce(
            a.acuity_desc ilike any ('%standard%', '%low acuity%', '%non-urgent%', '%non urgent%')
            and a.discharge_destination_group = 'Non-admitted'
            , false
        ) as is_possibly_manageable_elsewhere
        , coalesce(
            a.discharge_destination_code = '305398007' -- mortuary
            or a.discharge_status_desc ilike '%died%'
            , false
        ) as is_died
    from attendances a
    inner join inclusion_list il
        on a.sk_patient_id = il.patient_id
    left join alcohol_drug_attendances adi
        on a.visit_occurrence_id = adi.visit_occurrence_id
),

admission_events as (
    select il.patient_id
        , il.area_code
        , 'NEL' as event_setting
        , a.visit_occurrence_id::varchar as event_id
        , a.start_date as event_date
        , a.start_time as event_time
        , coalesce(icd.description, a.primary_diagnosis_code) as event_reason
        , null::varchar as arrival_mode_desc
        , null::varchar as attendance_source_desc
        , a.duration as length_of_stay_days
        , null::boolean as is_ambulance_arrival
        , asa.visit_occurrence_id is not null as is_ambulatory_care_sensitive
        , true as is_admitted
        , coalesce(left(a.icd10_code, 3) between 'F20' and 'F99', false) as is_mental_health
        , coalesce(left(a.icd10_code, 3) between 'F10' and 'F19', false) as is_alcohol_drug
        , coalesce(left(a.icd10_code, 3) between 'W00' and 'W19' or left(a.icd10_code, 4) = 'R296', false) as is_fall
        , asa.visit_occurrence_id is not null as is_ltc_flare
        , false as is_possibly_manageable_elsewhere
        , coalesce(a.discharge_method_code = '4', false) as is_died
    from admissions a
    inner join inclusion_list il
        on a.sk_patient_id = il.patient_id
    left join ambulatory_sensitive_admissions asa
        on a.visit_occurrence_id = asa.visit_occurrence_id
    left join {{ ref('icd10_code') }} icd
        on a.icd10_code = icd.code
)

select * from attendance_events
union all
select * from admission_events
