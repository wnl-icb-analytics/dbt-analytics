-- Diagnostic coverage comparison, not an assertion that populations are identical.
-- Compare all commissioners before selecting a cohort: monthly uses submitted codes,
-- while established OBTs use CAM-first assignment. AE and ECDS differ in source scope.
-- No episode OBT exists, so APC episodes use the established episode staging interface.
with monthly as (
    select 'OP' as dataset, activity_date, organisation_id as provider_code
    from {{ ref('fct_sus_op_monthly_appointment') }}
    union all
    select 'AE', activity_date, organisation_id
    from {{ ref('fct_sus_ae_monthly_attendance') }}
    union all
    select 'APC spell', activity_date, organisation_id
    from {{ ref('fct_sus_apc_monthly_spell') }}
    union all
    select 'APC episode', activity_date, organisation_id
    from {{ ref('fct_sus_apc_monthly_episode') }}
), established as (
    select 'OP' as dataset, start_date::date as activity_date, organisation_id as provider_code
    from {{ ref('obt_appointment_op') }}
    union all
    select 'AE', start_date::date, organisation_id
    from {{ ref('obt_encounter_uec') }}
    union all
    select 'APC spell', end_date::date, organisation_id
    from {{ ref('obt_encounter_apc') }}
    union all
    select 'APC episode', end_date::date, commissioning_service_agreement_provider
    from {{ ref('stg_sus_apc_spell_episodes') }}
), monthly_counts as (
    select dataset, date_trunc('month', activity_date)::date as activity_month,
        coalesce(nullif(upper(trim(provider_code)), ''), '[NULL]') as provider_code,
        count(*) as row_count
    from monthly
    where activity_date >= '{{ var("sus_monthly_comparison_start_date", "2020-04-01") }}'::date
    group by all
), established_counts as (
    select dataset, date_trunc('month', activity_date)::date as activity_month,
        coalesce(nullif(upper(trim(provider_code)), ''), '[NULL]') as provider_code,
        count(*) as row_count
    from established
    where activity_date >= '{{ var("sus_monthly_comparison_start_date", "2020-04-01") }}'::date
    group by all
), coverage as (
    select dataset, max(activity_month) as latest_month
    from monthly_counts
    group by dataset
)
select
    coalesce(m.dataset, e.dataset) as dataset,
    coalesce(m.activity_month, e.activity_month) as activity_month,
    coalesce(m.provider_code, e.provider_code) as provider_code,
    coalesce(e.row_count, 0) as established_rows,
    coalesce(m.row_count, 0) as monthly_rows,
    coalesce(m.row_count, 0) - coalesce(e.row_count, 0) as monthly_minus_established,
    100.0 * (coalesce(m.row_count, 0) - coalesce(e.row_count, 0)) / nullif(e.row_count, 0) as difference_pct,
    c.latest_month as latest_monthly_activity_month,
    coalesce(m.activity_month, e.activity_month) > c.latest_month as beyond_monthly_coverage
from monthly_counts as m
full outer join established_counts as e
    on m.dataset = e.dataset
    and m.activity_month = e.activity_month
    and m.provider_code = e.provider_code
left join coverage as c
    on coalesce(m.dataset, e.dataset) = c.dataset
order by dataset, activity_month, provider_code
