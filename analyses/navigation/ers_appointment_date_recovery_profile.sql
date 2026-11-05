-- Diagnostic cohort only: two years is not a specification booking limit.
with suspect as (
    select appointment_id, ubrn_id, service_id, appointment_at,
        provider_organisation_code, first_observed_action_at
    from {{ ref('fct_ers_appointment') }}
    where appointment_at > dateadd(year, 2, current_date())
), candidates as (
    select
        p.appointment_id, p.ubrn_id, p.service_id, p.appointment_at,
        p.first_observed_action_at,
        min(r.acute_record_date) as candidate_date,
        count(distinct r.acute_record_date) as candidate_date_count
    from suspect as p
    inner join {{ ref('rel_ers_referral_acute_record') }} as r
        on p.ubrn_id = r.ubrn_id
        and r.acute_source = 'sus_outpatient'
        and r.patient_key_agreement = 'agree'
        and month(r.acute_record_date) = month(p.appointment_at)
        and day(r.acute_record_date) = day(p.appointment_at)
    inner join {{ ref('stg_sus_op_appointment') }} as op
        on r.acute_source_record_id = op.primarykey_id::varchar
        and op.appointment_time = p.appointment_at::time
        and op.appointment_commissioning_service_agreement_provider = p.provider_organisation_code
    group by p.appointment_id, p.ubrn_id, p.service_id, p.appointment_at, p.first_observed_action_at
), assessed as (
    select
        c.appointment_id, c.appointment_at, c.first_observed_action_at,
        c.candidate_date, c.candidate_date_count,
        count(distinct existing.appointment_id) as existing_corrected_slot_count,
        count(distinct competing.appointment_id) as competing_candidate_count
    from candidates as c
    left join {{ ref('fct_ers_appointment') }} as existing
        on c.ubrn_id = existing.ubrn_id
        and equal_null(c.service_id, existing.service_id)
        and existing.appointment_id <> c.appointment_id
        and existing.appointment_at = timestamp_ntz_from_parts(c.candidate_date, c.appointment_at::time)
    left join candidates as competing
        on c.ubrn_id = competing.ubrn_id
        and equal_null(c.service_id, competing.service_id)
        and c.candidate_date = competing.candidate_date
        and c.appointment_at::time = competing.appointment_at::time
    group by c.appointment_id, c.appointment_at, c.first_observed_action_at,
        c.candidate_date, c.candidate_date_count
)
select
    (select count(*) from suspect) as suspect_slot_count,
    count(*) as corroborated_candidate_count,
    count_if(candidate_date_count = 1) as unique_candidate_date_count,
    count_if(candidate_date >= first_observed_action_at::date) as candidate_not_before_first_action_count,
    count_if(existing_corrected_slot_count > 0) as existing_corrected_slot_count,
    count_if(competing_candidate_count > 1) as competing_suspect_slot_count
from assessed
