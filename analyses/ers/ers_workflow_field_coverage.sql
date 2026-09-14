-- One row per supplied action code. Sparse fields have action-specific denominators.
select action_code, max(action_name) as action_name,
    count(*) as action_rows, count(distinct ubrn_id) as requests,
    min(action_at)::date as first_action_date, max(action_at)::date as last_action_date,
    count(sk_patient_id) as patient_key_rows,
    count(pathway_started_at) as pathway_start_rows,
    count(service_id) as service_rows, count(service_specialty_code) as service_specialty_rows,
    count(specialty_code) as search_specialty_rows,
    count(action_reason_code) as reason_rows, count(appointment_at) as appointment_time_rows,
    count(advice_request_status_code) as advice_status_rows,
    count(assessment_outcome_code) as assessment_outcome_rows,
    count_if(pathway_started_at > action_at) as pathway_after_action_rows
from {{ ref('fct_ers_referral_action') }}
group by action_code
having count(*) >= 100
order by action_code
