{{
    config(
        materialized='table',
        tags=['daily']
    )
}}

/* Analyst-facing source content-currency contract. */

with source_signals as (
    select *
    from {{ ref('stg_source_content_freshness') }}
),

ages as (
    select
        *,
        datediff(day, consensus_content_date, current_date()) as content_age_days,
        dateadd(day, expected_days, consensus_content_date)::date as expected_by_date,
        case
            when sla_days is not null
                then dateadd(day, sla_days, consensus_content_date)::date
        end as sla_by_date,
        dateadd(day, breach_after_days, consensus_content_date)::date as breach_by_date
    from source_signals
),

analyst_signals as (
    select
        *,
        greatest(content_age_days - expected_days, 0) as days_past_expected,
        case when sla_days is not null then greatest(content_age_days - sla_days, 0) end as days_past_sla,
        greatest(content_age_days - breach_after_days, 0) as days_past_breach,
        greatest(breach_after_days - content_age_days, 0) as days_until_breach,
        content_age_days > expected_days as is_past_expected,
        sla_days is not null and content_age_days > sla_days as is_sla_missed,
        content_age_days >= breach_after_days as is_breached,
        case
            when content_age_days >= breach_after_days then 'breached'
            when sla_days is not null and content_age_days > sla_days then 'sla_missed'
            when content_age_days > expected_days then 'past_expected'
            else 'current'
        end as freshness_status
    from ages
)

select *
from analyst_signals
