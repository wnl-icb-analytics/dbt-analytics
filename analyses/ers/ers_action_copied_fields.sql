-- Whole-table fingerprints compare copied fields without returning their values.
with supplied as (
    select
        a.action_id as action_id,
        a.ubrn_id as ubrn_id,
        a.ubrn as ubrn,
        nullif(replace(trim(a.ubrn), '-', ''), '') as normalised_ubrn,
        {{ consistent_sk_patient_id_format('a.nhs_number_pseudo') }} as sk_patient_id,
        a.e_referral_pathway_start as pathway_started_at,
        a.action_dt_tm as action_at,
        a.action_cd::varchar as action_code,
        a.action_reason_cd::varchar as action_reason_code,
        a.priority_cd::varchar as priority_code,
        a.specialty_cd::varchar as specialty_code,
        a.clinic_type_cd::varchar as clinic_type_code,
        a.referring_org_id as referring_organisation_code,
        a.org_id as action_organisation_code,
        a.service_id as service_id,
        a.service_specialty_cd::varchar as service_specialty_code,
        a.provider_org_id as provider_organisation_code,
        a.location_org_id as site_code,
        a.appt_dt_tm as appointment_at,
        a.appt_type_cd::varchar as appointment_type_code,
        a.rebooked_to_action_id as rebooked_to_action_id,
        a.initial_ubrn_id as initial_ubrn_id,
        a.previous_ubrn_id as previous_ubrn_id,
        a.initial_ubrn as initial_ubrn,
        a.previous_ubrn as previous_ubrn,
        a.next_ubrn as next_ubrn,
        a.clinical_assessment_outcome_cd::varchar as assessment_outcome_code,
        a.ar_status_cd::varchar as advice_request_status_code,
        a.uniq_submission_id as source_submission_id,
        a.dmic_date_added as source_imported_at
    from {{ ref('stg_ers_ubrn_action') }} as a
), published as (
    select action_id, ubrn_id, ubrn, normalised_ubrn, sk_patient_id, pathway_started_at, action_at, action_code, action_reason_code, priority_code, specialty_code, clinic_type_code, referring_organisation_code, action_organisation_code, service_id, service_specialty_code, provider_organisation_code, site_code, appointment_at, appointment_type_code, rebooked_to_action_id, initial_ubrn_id, previous_ubrn_id, initial_ubrn, previous_ubrn, next_ubrn, assessment_outcome_code, advice_request_status_code, source_submission_id, source_imported_at
    from {{ ref('fct_ers_referral_action') }}
)
select 'source' as population, count(*) as row_count, hash_agg(*) as content_hash from supplied
union all
select 'fact' as population, count(*) as row_count, hash_agg(*) as content_hash from published
