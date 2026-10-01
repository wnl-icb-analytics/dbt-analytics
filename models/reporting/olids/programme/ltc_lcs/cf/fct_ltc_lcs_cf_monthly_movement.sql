{{
    config(
        materialized='table',
        cluster_by=['to_month_end_date']
    )
}}

-- LTC LCS: month-to-month movement into and out of each case-finding indicator.
-- One row per person per indicator per to_month_end_date, comparing membership at
-- to_month_end_date against the immediately prior populated month:
--   Entered  - member this month, not the prior populated month (or first month)
--   Retained - member both this month and the prior populated month
--   Exited   - member the prior populated month, not this month
-- Populated months come from the membership grain, so a one-month gap is handled
-- correctly rather than assuming contiguous calendar months.
--
-- Table (not incremental): see int_ltc_lcs_cf_monthly_membership.sql for rationale.

with membership as (
    select person_id, month_end_date, condition, indicator_code
    from {{ ref('int_ltc_lcs_cf_monthly_membership') }}
),

populated_months as (
    select
        month_end_date,
        lag(month_end_date) over (order by month_end_date) as prior_month_end_date,
        lead(month_end_date) over (order by month_end_date) as next_month_end_date
    from (select distinct month_end_date from membership)
),

-- Members this month: Retained if also a member the prior populated month, else Entered.
entered_retained as (
    select
        m.person_id,
        m.condition,
        m.indicator_code,
        m.month_end_date as to_month_end_date,
        case
            when pm.prior_month_end_date is null then 'Entered'
            when prior.person_id is not null then 'Retained'
            else 'Entered'
        end as movement_type
    from membership m
    inner join populated_months pm on m.month_end_date = pm.month_end_date
    left join membership prior
        on prior.person_id = m.person_id
        and prior.indicator_code = m.indicator_code
        and prior.month_end_date = pm.prior_month_end_date
),

-- Members a given month who are not members the next populated month: Exited at that next month.
exited as (
    select
        m.person_id,
        m.condition,
        m.indicator_code,
        pm.next_month_end_date as to_month_end_date,
        'Exited' as movement_type
    from membership m
    inner join populated_months pm on m.month_end_date = pm.month_end_date
    left join membership nxt
        on nxt.person_id = m.person_id
        and nxt.indicator_code = m.indicator_code
        and nxt.month_end_date = pm.next_month_end_date
    where pm.next_month_end_date is not null
      and nxt.person_id is null
)

select person_id, condition, indicator_code, to_month_end_date, movement_type
from entered_retained
union all
select person_id, condition, indicator_code, to_month_end_date, movement_type
from exited
