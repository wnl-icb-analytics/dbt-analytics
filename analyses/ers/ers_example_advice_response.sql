-- First recorded advice request per UBRN/service and its first later response by action ID.
-- All available response dates are searched before grouping by request month.
with requests as (
    select ubrn_id, service_id, action_id, action_at
    from {{ ref('fct_ers_referral_action') }}
    where action_code = '1407'
    qualify row_number() over (partition by ubrn_id, service_id order by action_id) = 1
), responses as (
    select r.ubrn_id, r.service_id, r.action_at as request_at, a.action_at as response_at
    from requests r
    left join {{ ref('fct_ers_referral_action') }} a
        on r.ubrn_id = a.ubrn_id
        and r.service_id = a.service_id
        and a.action_code = '1408' and a.action_id > r.action_id
    qualify row_number() over (partition by r.ubrn_id, r.service_id order by a.action_id nulls last) = 1
)
select date_trunc('month', request_at)::date as request_month,
    count(*) as request_service_groups,
    count_if(service_id is null) as missing_service_groups,
    count(response_at) as groups_with_later_response,
    count_if(response_at < request_at) as reversed_timestamp_groups,
    count_if(response_at is null) as groups_without_observed_response,
    median(case when response_at >= request_at then datediff('minute', request_at, response_at) / 1440.0 end)
        as median_elapsed_calendar_days
from responses
group by request_month
having count(*) >= 100
order by request_month
