-- Recorded triage actions, not completed RAS pathways or an open-worklist count.
-- NHS guidance: 1608/1770 are RAS; 1810 is booking-deferral (ASI) outcome.
-- Dictionary meaning identifies 1836 as booking-review outcome, despite the shared display.
select date_trunc('month', action_at)::date as action_month,
    action_code, max(action_name) as action_name,
    action_reason_code, max(action_reason_name) as action_reason_name,
    count(*) as recorded_actions, count(distinct ubrn_id) as requests_with_action,
    count(service_id) as service_rows, count(service_specialty_code) as service_specialty_rows
from {{ ref('fct_ers_referral_action') }}
where action_code in ('1608', '1770', '1810', '1836')
group by action_month, action_code, action_reason_code
having count(*) >= 100
order by action_month, action_code, action_reason_code
