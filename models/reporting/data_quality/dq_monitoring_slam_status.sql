{{ config(
    materialized='view'
) }}

-- =====================================================
-- SLAM DQ Monitoring - Status
--
-- Applies initial DQ status logic to the enriched
-- SLAM variance layer.
--
-- Source:
--   dq_monitoring_slam_variance
--
-- Grain:
--   measurement_date
--   + table_id
--   + provider_code
--
-- Initial status philosophy:
--
-- RED:
--   Entire activity or financial measure is NULL.
--
-- AMBER:
--   Partial activity or financial NULLs are present.
--
-- GREEN:
--   No activity or financial NULLs are present.
--
-- Activity / financial movement is retained for
-- investigation but does not yet independently drive
-- status while the historical baseline is immature.
-- =====================================================


with status_logic as (

    select
        *,

        -- =================================================
        -- Activity completeness status
        -- =================================================

        case
            when row_count > 0
             and null_activity_count = row_count
                then 'RED'

            when null_activity_count > 0
                then 'AMBER'

            else 'GREEN'
        end as activity_completeness_status,


        -- =================================================
        -- Financial completeness status
        -- =================================================

        case
            when row_count > 0
             and null_financial_count = row_count
                then 'RED'

            when null_financial_count > 0
                then 'AMBER'

            else 'GREEN'
        end as financial_completeness_status

    from {{ ref('dq_monitoring_slam_variance') }}

),


overall_status as (

    select
        *,

        -- =================================================
        -- Overall SLAM DQ status
        --
        -- Worst component status wins.
        -- =================================================

        case
            when activity_completeness_status = 'RED'
              or financial_completeness_status = 'RED'
                then 'RED'

            when activity_completeness_status = 'AMBER'
              or financial_completeness_status = 'AMBER'
                then 'AMBER'

            else 'GREEN'
        end as dq_status,


        -- =================================================
        -- Human-readable status reason
        -- =================================================

        case
            when activity_completeness_status = 'RED'
             and financial_completeness_status = 'RED'
                then
                    'All activity and financial values are null'

            when activity_completeness_status = 'RED'
                then
                    'All activity values are null'

            when financial_completeness_status = 'RED'
                then
                    'All financial values are null'

            when activity_completeness_status = 'AMBER'
             and financial_completeness_status = 'AMBER'
                then
                    'Partial activity and financial nulls detected'

            when activity_completeness_status = 'AMBER'
                then
                    'Partial nulls detected'

            when financial_completeness_status = 'AMBER'
                then
                    'Partial nulls detected'

            else
                'No nulls detected'
        end as status_reason

    from status_logic

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

    previous_measurement_date,
    previous_financial_year,
    previous_financial_month,
    reporting_period_changed,

    row_count,
    previous_row_count,
    row_count_change,
    row_count_change_pct,

    activity_value,
    previous_activity_value,
    activity_change,
    activity_change_pct,

    financial_value,
    previous_financial_value,
    financial_change,
    financial_change_pct,

    null_activity_count,
    null_activity_pct,
    previous_null_activity_pct,
    null_activity_pct_change,

    null_financial_count,
    null_financial_pct,
    previous_null_financial_pct,
    null_financial_pct_change,

    latest_refresh_date,
    latest_update_date,

    activity_completeness_status,
    financial_completeness_status,

    dq_status,
    status_reason

from overall_status
