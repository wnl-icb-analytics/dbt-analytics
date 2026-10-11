/*
Per-person patient-attributable spend by month for the resource index.

Sources: SLAM (acute actuals), EPD (GP prescribing actuals), MHSDS and CSDS
(currency x price proxies). SLAM mental health and community block lines are
mostly not patient-attributable, so the proxy sources do not double count.

GP appointment cost is excluded: OLIDS gives person-level appointments for
NCL only, and NWL has no person-level GP feed. Adding it to one sub-ICB would
bias every NCL vs NWL comparison.

Months run from the latest first month to the earliest last month across the
four sources, so every month carries all of them. EPD lags SLAM by about two
months, which pulls the resource index window back accordingly.
*/
{{ config(materialized = 'table') }}

with source_cost as (
    select
        sk_patient_id,
        activity_month,
        cost_source,
        total_cost
    from {{ ref('fct_person_cost_index_monthly') }}
    where is_patient_attributable
      and sk_patient_id is not null
      and cost_source in ('SLAM', 'EPD', 'MHSDS', 'CSDS')
),

bounds as (
    select
        max(min_month) as start_month,
        min(max_month) as end_month
    from (
        select
            cost_source,
            min(activity_month) as min_month,
            max(activity_month) as max_month
        from source_cost
        group by 1
    )
)

select
    c.sk_patient_id,
    c.activity_month,
    sum(coalesce(c.total_cost, 0))                                    as actual_cost,
    sum(iff(c.cost_source = 'SLAM', coalesce(c.total_cost, 0), 0))    as slam_cost,
    sum(iff(c.cost_source = 'EPD', coalesce(c.total_cost, 0), 0))     as epd_cost,
    sum(iff(c.cost_source = 'MHSDS', coalesce(c.total_cost, 0), 0))   as mhsds_cost,
    sum(iff(c.cost_source = 'CSDS', coalesce(c.total_cost, 0), 0))    as csds_cost
from source_cost as c
cross join bounds as b
where c.activity_month between b.start_month and b.end_month
group by 1, 2
