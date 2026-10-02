/*
ECDS attendance fields and measures used for the NWL QuickReport review.

One row per ECDS attendance. Clinical child feeds are reduced to attendance
grain before joining. The practice is the one submitted for the attendance;
PCN and neighbourhood use the latest maintained mapping for that practice.
*/

with mental_health_diagnosis_attendances as (
    select distinct visit_occurrence_id
    from {{ ref('int_sus_uec_diagnosis') }}
    where source_concept_code in (
        '52448006', '2776000', '33449004', '72366004', '197480006'
        , '35489007', '13746004', '58214004', '69322001', '397923000'
        , '30077003', '44376007', '17226007', '50705009'
    )
),

psychiatric_referral_attendances as (
    select distinct visit_occurrence_id
    from {{ ref('int_sus_uec_referred_to_service') }}
    where referred_to_service_ecds_group1 = 'Psychiatric'
),

discharge_destination_groups as (
    select
        snomed_code
        , max(ecds_group1) as ecds_group1
    from {{ ref('stg_dictionary_ecds_dischargedestination') }}
    group by snomed_code
),

attendances as (
    select
        a.*
        , p.pcn_name as reg_practice_pcn_name_latest
        , p.neighbourhood_name as reg_practice_neighbourhood_name_latest
        , r.visit_occurrence_id is not null as is_mental_health_referral
        , coalesce(
            a.age_at_event < 18
            and (
                d.visit_occurrence_id is not null
                or a.chief_complaint_code in (
                    '248062006', '272022009', '366979004', '48694002'
                    , '248020004', '6471006', '7011001'
                )
                or a.injury_intent_code = '276853009'
            )
            , false
        ) as is_cyp_mental_health
        , dd.ecds_group1 as discharge_destination_ecds_group1
        , coalesce(
            dd.ecds_group1 in ('Admitted', 'Transfer')
            or a.discharge_destination_code in ('1066331000000109', '1066341000000100')
            , false
        ) as is_admitted
        , coalesce(
            dd.ecds_group1 not in ('Admitted', 'Transfer')
            and a.discharge_destination_code not in ('1066331000000109', '1066341000000100')
            , false
        ) as is_non_admitted
    from {{ ref('obt_encounter_uec') }} as a
    left join mental_health_diagnosis_attendances as d
        on a.visit_occurrence_id = d.visit_occurrence_id
    left join psychiatric_referral_attendances as r
        on a.visit_occurrence_id = r.visit_occurrence_id
    left join {{ ref('stg_reference_primary_care_practice_all') }} as p
        on a.reg_practice_at_event = p.practice_code
    left join discharge_destination_groups as dd
        on a.discharge_destination_code = dd.snomed_code
)

select
    *
    , case
        when site_id in ('AD915', 'AD904', 'AD906', 'NLO21', 'AD918', 'RY901')
            and department_type in ('3', '03')
            and discharge_status_code in ('1077031000000103', '1077781000000101')
            then 0
        else 1
        end as attendance_count
    , iff(is_admitted, 1, 0) as admitted_count
    , iff(is_non_admitted, 1, 0) as non_admitted_count
    , case
        when duration > 720
            and attendance_category_code not in ('04', '4', 'X')
            and discharge_status_code <> '63238001'
            and (end_date is not null or end_time is not null)
            then 1
        else 0
        end as over_12_hours_count
    , case
        when duration <= 720
            and attendance_category_code not in ('04', '4', 'X')
            and discharge_status_code <> '63238001'
            and (end_date is not null or end_time is not null)
            then 1
        else 0
        end as within_12_hours_count
    , iff(duration > 4320, 1, 0) as over_72_hours_count
    , iff(initial_assessment_time_since_arrival <= 15, 1, 0) as assessed_within_15_minutes_count
    , iff(initial_assessment_time_since_arrival > 15, 1, 0) as not_assessed_within_15_minutes_count
    , iff(is_admitted, duration, null) as admitted_duration_minutes
    , iff(is_non_admitted, duration, null) as non_admitted_duration_minutes
from attendances
