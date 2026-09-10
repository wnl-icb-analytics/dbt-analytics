{{ config(tags=['ers', 'large_periodic'], cluster_by=['sk_patient_id', 'pathway_started_at']) }}

with referral_history as (
    select
        ubrn_id,
        min(action_at) as first_observed_action_at,
        max(action_at) as last_observed_action_at,
        count(*) as action_count,
        min(action_id) as first_action_id,
        max(action_id) as latest_action_id,
        max(case when service_id is not null then action_id end) as last_service_action_id,
        max_by(pathway_started_at, case when pathway_started_at is not null then action_id end)
            as pathway_started_at,
        -- A conflicting patient key must not become a confirmed cross-source link.
        case when count(distinct sk_patient_id) = 1 then min(sk_patient_id) end as sk_patient_id,
        count(distinct service_id) as recorded_service_count
    from {{ ref('fct_ers_referral_action') }}
    group by ubrn_id
)

select
    h.ubrn_id,
    a.ubrn,
    a.normalised_ubrn,
    h.sk_patient_id,
    h.pathway_started_at,
    h.first_observed_action_at,
    h.last_observed_action_at,
    h.action_count,
    h.first_action_id,
    h.latest_action_id,
    a.action_code as latest_action_code,
    a.action_name as latest_action_name,
    a.action_reason_code as latest_action_reason_code,
    a.action_reason_name as latest_action_reason_name,
    a.priority_code,
    a.priority_name,
    a.specialty_code,
    a.specialty_name,
    a.clinic_type_code,
    a.clinic_type_name,
    a.referring_organisation_code,
    a.referring_organisation_name,
    h.recorded_service_count,
    h.last_service_action_id,
    s.action_at as last_service_action_at,
    s.service_id as last_recorded_service_id,
    s.service_name as last_recorded_service_name,
    s.service_specialty_code as last_recorded_service_specialty_code,
    s.service_specialty_name as last_recorded_service_specialty_name,
    s.provider_organisation_code as last_recorded_provider_code,
    s.provider_organisation_name as last_recorded_provider_name,
    s.site_code as last_recorded_site_code,
    s.site_name as last_recorded_site_name,
    a.initial_ubrn_id,
    a.previous_ubrn_id,
    a.initial_ubrn,
    a.previous_ubrn,
    a.next_ubrn
from referral_history as h
inner join {{ ref('fct_ers_referral_action') }} as a
    on h.latest_action_id = a.action_id
left join {{ ref('fct_ers_referral_action') }} as s
    on h.last_service_action_id = s.action_id
