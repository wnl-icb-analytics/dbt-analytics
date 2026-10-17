{{ config(severity = 'error') }}

-- The shared model keeps history; only the resource fact's rolling window
-- must contain exactly 12 consecutive calendar months.
with cost_months as (
    select distinct activity_month
    from {{ ref('int_person_cost_index_actual_monthly') }}
),

bounds as (
    select
        max(activity_month) as end_month,
        dateadd(month, -11, max(activity_month)) as start_month
    from cost_months
),

window_months as (
    select c.activity_month
    from cost_months as c
    cross join bounds as b
    where c.activity_month between b.start_month and b.end_month
)

select
    count(*) as months_present,
    min(activity_month) as start_month,
    max(activity_month) as end_month
from window_months
having count(*) <> 12
    or datediff(month, min(activity_month), max(activity_month)) <> 11
