{{ config(materialized = 'table') }}

/*
Onward referrals from A&E attendances (ECDS referred-to service).

One row per referral record. An attendance can have several: a referral to
more than one service, or a re-referral after assessment. Nothing is
selected or discarded; count referrals here, not on the encounter model.
*/

select
    {{ dbt_utils.generate_surrogate_key(['r.primarykey_id', 'r.referred_to_id']) }} as referral_id
    , r.primarykey_id as visit_occurrence_id
    , sa.sk_patient_id
    , sa.start_date as attendance_date
    , sa.organisation_id
    , sa.site_id
    , sa.department_type
    , r.referred_to_id as referral_sequence
    , r.referred_to_service_code
    , d.snomed_uk_preferred_term as referred_to_service_desc
    , d.ecds_group1 as referred_to_service_ecds_group1
    , r.assessment_date
    , r.assessment_time
from {{ ref('stg_sus_ecds_attendance_referred_to') }} as r
left join {{ ref('int_sus_uec_encounter') }} as sa
    on r.primarykey_id = sa.visit_occurrence_id
left join {{ ref('stg_dictionary_ecds_referredtoservice') }} as d
    on r.referred_to_service_code = d.snomed_code
