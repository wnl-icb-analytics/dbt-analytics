-- First recorded receipt per request/provider, using the legacy receipt action set.
-- This is a local reporting definition, not an e-RS current worklist or referral grain.
with received as (
    select *
    from {{ ref('fct_ers_referral_action') }}
    where action_code in ('1412', '1430', '1608')
        and provider_organisation_code is not null
    qualify row_number() over (
        partition by ubrn_id, provider_organisation_code
        order by action_at, action_id
    ) = 1
)
select financial_year, financial_month, max(financial_month_name) as financial_month_name,
    provider_organisation_code, max(provider_organisation_name) as provider_organisation_name,
    count(*) as request_provider_receipts,
    count(patient_age) as receipts_with_recorded_age,
    count(registered_practice_code) as receipts_with_recorded_practice,
    count(residence_imd_2019_decile) as receipts_with_imd_2019
from received
where action_at >= '2019-04-01'::date
group by financial_year, financial_month, provider_organisation_code
having count(*) >= 100
order by financial_year, financial_month, provider_organisation_code
