{{ config(
    materialized = 'view'
) }}

-- =====================================================
-- SLAM DQ Monitoring - Current Metrics
--
-- Produces enriched SLAM measures for the ten configured
-- WNL ICB core providers, restricted to the latest
-- submitted financial period for each feed.
--
-- Period selection:
--   int_ls_contract_dataset_submissions
--
-- Provider selection:
--   dq_monitoring_provider_config
--
-- Outputs one row per:
--   table_id + provider + financial period
-- =====================================================


with slam_providers as (

    -- =================================================
    -- Retain only the configured SLAM providers that
    -- should be monitored individually.
    -- =================================================

    select
        provider_code,
        provider_name,
        alert_enabled

    from {{ ref('dq_monitoring_provider_config') }}

    where dataset_group = 'SLAM'
      and provider_scope = 'CORE'
      and monitor_individually = true

),


acm_latest_period as (

    -- =================================================
    -- Latest submitted ACM actual financial period.
    --
    -- This deliberately reuses the established
    -- contracting submission model rather than deriving
    -- the latest period from the contents of the ACM
    -- source itself.
    -- =================================================

    select
        financial_year,
        fy_month_submitted

    from {{ ref('int_ls_contract_dataset_submissions') }}

    where dataset = 'LSACM'
      and financial_year is not null
      and fy_month_submitted is not null

    qualify row_number() over (
        order by
            financial_year desc,
            fy_month_submitted desc
    ) = 1

),


plcm_latest_period as (

    -- =================================================
    -- Latest submitted PLCM actual financial period.
    -- =================================================

    select
        financial_year,
        fy_month_submitted

    from {{ ref('int_ls_contract_dataset_submissions') }}

    where dataset = 'LSPLCM'
      and financial_year is not null
      and fy_month_submitted is not null

    qualify row_number() over (
        order by
            financial_year desc,
            fy_month_submitted desc
    ) = 1

),


acm_metrics as (

    -- =================================================
    -- Aggregate Contract Monitoring
    --
    -- DV_ACTUAL_ACTIVITY:
    --   Submitted actual activity.
    --
    -- DV_ACTUAL_PRICE:
    --   Submitted actual financial value.
    -- =================================================

    select
        current_date()                         as measurement_date,
        current_timestamp()                    as measured_at,

        'SLAM_LSACM'                           as table_id,

        a.dv_provider_code                     as provider_code,
        sp.provider_name                       as provider_name,
        sp.alert_enabled                       as alert_enabled,

        a.dv_financial_year::integer           as financial_year,
        a.dv_financial_month::integer          as financial_month,

        count(*)                               as row_count,

        sum(a.dv_actual_activity)              as activity_value,
        sum(a.dv_actual_price)                 as financial_value,

        count_if(
            a.dv_actual_activity is null
        )                                      as null_activity_count,

        count_if(
            a.dv_actual_price is null
        )                                      as null_financial_count,

        round(
            100.0
            * count_if(a.dv_actual_activity is null)
            / nullif(count(*), 0),
            2
        )                                      as null_activity_pct,

        round(
            100.0
            * count_if(a.dv_actual_price is null)
            / nullif(count(*), 0),
            2
        )                                      as null_financial_pct,

        max(
            case
                when a.dv_dataset_created_at
                        <= current_timestamp()
                then to_date(a.dv_dataset_created_at)
            end
        )                                      as latest_refresh_date,

        max(
            to_date(a.submission_loaded_at)
        )                                      as latest_update_date

    from {{ ref('stg_lsacm_latest') }} a

    inner join slam_providers sp
        on a.dv_provider_code = sp.provider_code

    inner join acm_latest_period lp
        on try_to_number(
            left(a.dv_financial_year, 4)
        ) = lp.financial_year

       and a.dv_financial_month
            = lp.fy_month_submitted

    where a.dv_provider_code is not null

    group by
        a.dv_provider_code,
        sp.provider_name,
        sp.alert_enabled,
        a.dv_financial_year,
        a.dv_financial_month

),


plcm_metrics as (

    -- =================================================
    -- Patient Level Contract Monitoring
    --
    -- DV_ACTIVITY_COUNT:
    --   Submitted patient-level activity.
    --
    -- DV_TOTAL_COST:
    --   Submitted total financial value.
    --
    -- DV_ACTIVITY_UNIT_PRICE is deliberately not summed
    -- because unit prices are not additive.
    -- =================================================

    select
        current_date()                         as measurement_date,
        current_timestamp()                    as measured_at,

        'SLAM_LSPLCM'                          as table_id,

        p.dv_provider_code                     as provider_code,
        sp.provider_name                       as provider_name,
        sp.alert_enabled                       as alert_enabled,

        p.dv_financial_year::integer           as financial_year,
        p.dv_financial_month::integer          as financial_month,

        count(*)                               as row_count,

        sum(p.dv_activity_count)               as activity_value,
        sum(p.dv_total_cost)                   as financial_value,

        count_if(
            p.dv_activity_count is null
        )                                      as null_activity_count,

        count_if(
            p.dv_total_cost is null
        )                                      as null_financial_count,

        round(
            100.0
            * count_if(p.dv_activity_count is null)
            / nullif(count(*), 0),
            2
        )                                      as null_activity_pct,

        round(
            100.0
            * count_if(p.dv_total_cost is null)
            / nullif(count(*), 0),
            2
        )                                      as null_financial_pct,

        max(
            case
                when p.dv_dataset_created_at
                        <= current_timestamp()
                then to_date(p.dv_dataset_created_at)
            end
        )                                      as latest_refresh_date,

        max(
            to_date(p.submission_loaded_at)
        )                                      as latest_update_date

    from {{ ref('stg_lsplcm_latest') }} p

    inner join slam_providers sp
        on p.dv_provider_code = sp.provider_code

    inner join plcm_latest_period lp
        on try_to_number(
            left(p.dv_financial_year, 4)
        ) = lp.financial_year

       and p.dv_financial_month
            = lp.fy_month_submitted

    where p.dv_provider_code is not null

    group by
        p.dv_provider_code,
        sp.provider_name,
        sp.alert_enabled,
        p.dv_financial_year,
        p.dv_financial_month

),


combined as (

    -- =================================================
    -- Shared output structure for ACM and PLCM.
    -- =================================================

    select * from acm_metrics

    union all

    select * from plcm_metrics

)


select
    measurement_date,
    measured_at,
    table_id,
    provider_code,
    provider_name,
    alert_enabled,
    financial_year,
    financial_month,
    row_count,
    activity_value,
    financial_value,
    null_activity_count,
    null_financial_count,
    null_activity_pct,
    null_financial_pct,
    latest_refresh_date,
    latest_update_date

from combined
