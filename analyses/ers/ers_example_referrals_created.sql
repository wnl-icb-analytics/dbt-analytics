-- First observed Referral Created action per request, not all UBRNs or all referral actions.
with created as (
    select ubrn_id, action_at, referring_organisation_code, sk_patient_id
    from {{ ref('fct_ers_referral_action') }}
    where action_code = '1422'
    qualify row_number() over (partition by ubrn_id order by action_id) = 1
)
select date_trunc('month', action_at)::date as created_month,
    count(*) as requests_with_creation,
    count(sk_patient_id) as requests_with_patient_key,
    count(referring_organisation_code) as requests_with_referrer
from created
group by created_month
having count(*) >= 100
order by created_month
