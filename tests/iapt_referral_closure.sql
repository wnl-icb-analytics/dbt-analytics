-- Referral closure rules in fct_iapt_referral. The 2-month grace is recomputed here with month-boundary
-- datediff, independently of the model expression. Returns one aggregate row per failing rule; no record
-- identifiers.
with latest as (
    select max(reporting_period_end_date) as latest_period_end_date
    from {{ ref('stg_iapt_activesubmission') }}
    where reporting_period_end_date is not null
        and provider_organisation_code is not null
)

, expected as (
    select
        r.service_discharge_date
        , r.referral_received_date
        , r.last_reported_period_end_date
        , r.referral_status
        , r.referral_end_date
        , r.referral_end_date_source
        , case
            when r.service_discharge_date is not null then 'discharged'
            when datediff(month, r.last_reported_period_end_date, l.latest_period_end_date) <= 2 then 'open'
            else 'last_submission'
        end as expected_source
    from {{ ref('fct_iapt_referral') }} as r
    cross join latest as l
)

, failures as (
    -- A recorded discharge always wins and keeps its own date.
    select
        'actual_discharge_precedence' as rule_name
        , count_if(
            referral_status is distinct from 'discharged'
            or referral_end_date_source is distinct from 'discharged'
            or referral_end_date is distinct from service_discharge_date
        ) as failing_count
    from expected
    where service_discharge_date is not null

    union all

    -- Open inside the grace with no end date; closed outside it at the last reported month end.
    select
        'grace_boundary'
        , count_if(
            referral_end_date_source is distinct from expected_source
            or referral_status is distinct from iff(expected_source = 'open', 'open', 'closed')
            or referral_end_date is distinct from iff(expected_source = 'open', null, last_reported_period_end_date)
        )
    from expected
    where service_discharge_date is null

    union all

    select
        'inferred_end_before_referral_received'
        , count_if(referral_end_date < referral_received_date)
    from expected
    where referral_end_date_source = 'last_submission'
)

select
    rule_name
    , failing_count
from failures
where failing_count > 0
