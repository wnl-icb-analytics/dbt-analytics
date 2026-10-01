{{
    config(
        materialized='table',
        cluster_by=['to_month_end_date']
    )
}}

-- LTC LCS: person-level month-to-month movement between overall risk groups.
-- One row per person per to_month_end_date, comparing their risk group at
-- to_month_end_date against their risk group at their immediately prior
-- *populated* month (not necessarily the prior calendar month, since gaps are
-- possible if a person's snapshot row was invalidated and later reopened).
--
-- Table (not incremental): see int_ltc_lcs_rs_monthly_position.sql for why. Person
-- x month volume is currently small; revisit once it compounds across more months.

with positions as (
    select * from {{ ref('int_ltc_lcs_rs_monthly_position') }}
),

distinct_months as (
    select distinct month_end_date from positions
),

populated_months as (
    select
        month_end_date,
        lead(month_end_date) over (order by month_end_date) as next_month_end_date
    from distinct_months
),

person_month_with_lag as (
    select
        person_id,
        month_end_date,
        overall_risk_group,
        overall_risk_rank,
        lag(month_end_date) over (partition by person_id order by month_end_date) as prior_month_end_date,
        lag(overall_risk_group) over (partition by person_id order by month_end_date) as prior_risk_group,
        lag(overall_risk_rank) over (partition by person_id order by month_end_date) as prior_risk_rank,
        row_number() over (partition by person_id order by month_end_date desc) as recency_rank
    from positions
),

-- Every month a person actually appears in: their risk group vs. wherever they
-- were the last time they appeared (null from_* fields means new entrant).
entrants_and_continuers as (
    select
        person_id,
        month_end_date as to_month_end_date,
        prior_month_end_date as from_month_end_date,
        prior_risk_group as from_risk_group,
        prior_risk_rank as from_risk_rank,
        overall_risk_group as to_risk_group,
        overall_risk_rank as to_risk_rank
    from person_month_with_lag
),

-- The month after a person's last appearance, if that later month has actually
-- happened (next_month_end_date is not null) — confirms they dropped off rather
-- than just not having reached a future month yet.
leavers as (
    select
        pm.person_id,
        pop.next_month_end_date as to_month_end_date,
        pm.month_end_date as from_month_end_date,
        pm.overall_risk_group as from_risk_group,
        pm.overall_risk_rank as from_risk_rank,
        cast(null as varchar) as to_risk_group,
        cast(null as number) as to_risk_rank
    from person_month_with_lag pm
    inner join populated_months pop on pm.month_end_date = pop.month_end_date
    where pm.recency_rank = 1
      and pop.next_month_end_date is not null
),

combined as (
    select * from entrants_and_continuers
    union all
    select * from leavers
)

select
    person_id,
    to_month_end_date,
    from_month_end_date,
    from_risk_group,
    from_risk_rank,
    to_risk_group,
    to_risk_rank,
    case
        when from_month_end_date is null then 'New entrant'
        when to_risk_group is null then 'Left register'
        when to_risk_rank > from_risk_rank then 'Improved'
        when to_risk_rank < from_risk_rank then 'Worsened'
        else 'No change'
    end as movement_type
from combined
