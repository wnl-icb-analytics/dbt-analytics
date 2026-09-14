/*
MHSDS proxy cost per patient, calendar month, and service.

Grain: sk_patient_id x activity_month x service. Inpatient costs are
apportioned from fiscal-year bed-day segments to calendar months. Contact
activity counts every row; proxy_cost is zero for non-attended contacts.
*/

-- Spine bounds the cost index's monthly horizon: bed days accrued before
-- 2015-01 (legacy long-stay spells admitted decades ago) are deliberately
-- excluded, so monthly totals undercount those spells' full fct proxy_cost.
with month_spine as (
    select date_month::date as activity_month
    from (
        {{ dbt_utils.date_spine(
            datepart='month'
            , start_date="'2015-01-01'"
            , end_date="dateadd(month, 1, date_trunc('month', current_date))"
        ) }}
    )
)

, bed_day_segments as (
    select
        sk_patient_id
        , bed_days_from_date                                    as segment_start
        , bed_days_to_date                                      as segment_end
        , proxy_cost / nullif(bed_days, 0)                      as cost_per_bed_day
    from {{ ref('fct_mhsds_currency_bed_days') }}
    where sk_patient_id is not null
)

, bed_day_activity as (
    select
        b.sk_patient_id
        , m.activity_month
        , 'MH Inpatient'                                       as service
        , datediff(
            day
            , greatest(b.segment_start, m.activity_month)
            , least(b.segment_end, dateadd(month, 1, m.activity_month))
        ) * b.cost_per_bed_day                                 as total_cost
        , datediff(
            day
            , greatest(b.segment_start, m.activity_month)
            , least(b.segment_end, dateadd(month, 1, m.activity_month))
        )                                                      as total_activity
        , 'bed_day'                                            as activity_unit
    from bed_day_segments as b
    inner join month_spine as m
        on m.activity_month < b.segment_end
        and dateadd(month, 1, m.activity_month) > b.segment_start
)

, contact_activity as (
    select
        sk_patient_id
        , date_trunc('month', care_contact_date)::date         as activity_month
        , iff(
            is_crisis_referral
            , 'MH Crisis Contact'
            , 'MH Community Contact'
        )                                                      as service
        , proxy_cost                                           as total_cost
        , 1                                                    as total_activity
        , 'contact'                                            as activity_unit
    from {{ ref('fct_mhsds_currency_contacts') }}
    where sk_patient_id is not null
)

, combined as (
    select * from bed_day_activity
    union all
    select * from contact_activity
)

select
    sk_patient_id
    , activity_month
    , service
    , sum(total_cost)                                          as total_cost
    , sum(total_activity)                                      as total_activity
    , activity_unit
from combined
group by
    sk_patient_id
    , activity_month
    , service
    , activity_unit
