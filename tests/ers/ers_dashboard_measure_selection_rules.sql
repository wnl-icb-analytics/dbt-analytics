-- One-per-request measures select once; received rows have a provider; A&G responses carry a response time.
-- Returns failed rules with counts only.
with keyed as (
    select
        measure_code,
        ubrn_id,
        case measure_code
            when 'referrals_received_by_provider' then provider_organisation_code
            when 'referrals_accepted' then to_char(week_end_date, 'YYYY-MM-DD')
        end as selection_key,
        provider_organisation_code,
        advice_response_working_hours
    from {{ ref('fct_ers_dashboard_measure_event') }}
    where measure_code in (
        'referrals_accepted', 'advice_requests', 'advice_first_responses',
        'referrals_received_by_provider'
    )
), duplicates as (
    select measure_code, count(*) as failing_groups
    from (
        select measure_code
        from keyed
        group by measure_code, ubrn_id, selection_key
        having count(*) > 1
    )
    group by measure_code
)
select measure_code || ' selected more than once' as failed_rule, failing_groups
from duplicates

union all

select 'received without provider', count(*)
from keyed
where measure_code = 'referrals_received_by_provider'
    and provider_organisation_code is null
having count(*) > 0

union all

select 'advice response time missing or negative', count(*)
from keyed
where measure_code = 'advice_first_responses'
    and (advice_response_working_hours is null or advice_response_working_hours < 0)
having count(*) > 0
