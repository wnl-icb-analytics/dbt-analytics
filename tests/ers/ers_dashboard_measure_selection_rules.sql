-- Selected measures count each request (or request and provider) once; A&G responses carry a response time.
-- Returns failed rules with counts only.
with keyed as (
    select
        measure_code,
        ubrn_id,
        iff(measure_code = 'referrals_received_by_provider', provider_organisation_code, null) as provider_key,
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
        group by measure_code, ubrn_id, provider_key
        having count(*) > 1
    )
    group by measure_code
)
select measure_code || ' selected more than once' as failed_rule, failing_groups
from duplicates

union all

select 'advice response time missing or negative', count(*)
from keyed
where measure_code = 'advice_first_responses'
    and (advice_response_working_hours is null or advice_response_working_hours < 0)
having count(*) > 0
