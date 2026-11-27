-- As-of status rules in fct_iapt_referral_summary. The 2-month grace is recomputed here with month-boundary
-- datediff, independently of the model expression. Returns one aggregate row per failing rule; no record
-- identifiers.
with expected as (
    select
        service_discharge_date
        , referral_received_date
        , last_reported_period_end_date
        , as_of_referral_status
        , referral_end_date
        , referral_end_date_source
        , is_transfer_predecessor
        , case
            when service_discharge_date is not null then 'discharged'
            when datediff(month, last_reported_period_end_date, as_of_date) <= 2 then 'open'
            else 'no_longer_submitted'
        end as expected_status
    from {{ ref('fct_iapt_referral_summary') }}
)

, failures as (
    -- A recorded discharge always wins and keeps its own date.
    select
        'actual_discharge_precedence' as rule_name
        , count_if(
            as_of_referral_status is distinct from 'discharged'
            or referral_end_date_source is distinct from 'discharged'
            or referral_end_date is distinct from service_discharge_date
        ) as failing_count
    from expected
    where service_discharge_date is not null

    union all

    -- Open inside the grace with no end date; ended outside it at the last reported month end.
    select
        'grace_boundary'
        , count_if(
            as_of_referral_status is distinct from expected_status
            or referral_end_date_source is distinct from iff(expected_status = 'open', 'open', 'last_submission')
            or referral_end_date is distinct from iff(expected_status = 'open', null, last_reported_period_end_date)
        )
    from expected
    where service_discharge_date is null
        and not is_transfer_predecessor

    union all

    -- A referral continued under another provider code is transferred and ends at its last reported month.
    select
        'transferred_predecessor'
        , count_if(
            as_of_referral_status is distinct from 'transferred'
            or referral_end_date_source is distinct from 'last_submission'
            or referral_end_date is distinct from last_reported_period_end_date
        )
    from expected
    where is_transfer_predecessor

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
