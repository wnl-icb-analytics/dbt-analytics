{{ config(severity = 'error') }}

-- Check every expected source-month, including months absent from all sources.
-- Presence uses the same patient-attributable population as the shared costs.
with bounds as (
    select
        max(activity_month) as end_month,
        dateadd(month, -11, max(activity_month)) as start_month
    from {{ ref('int_person_cost_index_actual_monthly') }}
),

expected_months as (
    select dateadd(month, s.seq, b.start_month) as activity_month
    from (
        select row_number() over (order by null) - 1 as seq
        from table(generator(rowcount => 12))
    ) as s
    cross join bounds as b
),

required_sources as (
    select column1 as cost_source
    from values ('SLAM'), ('EPD'), ('MHSDS'), ('CSDS')
),

source_months as (
    select distinct c.cost_source, c.activity_month
    from {{ ref('fct_person_cost_index_monthly') }} as c
    cross join bounds as b
    where c.is_patient_attributable
      and c.sk_patient_id is not null
      and c.cost_source in ('SLAM', 'EPD', 'MHSDS', 'CSDS')
      and c.activity_month between b.start_month and b.end_month
)

select s.cost_source, m.activity_month
from expected_months as m
cross join required_sources as s
left join source_months as c
    on c.cost_source = s.cost_source
    and c.activity_month = m.activity_month
where c.cost_source is null
