{{ config(
    materialized='view'
) }}

-- =====================================================
-- SLAM DQ Monitoring - Variance
--
-- Compares each daily provider/feed observation with
-- the immediately preceding observation.
--
-- Source:
--   dq_monitoring_slam_history
--
-- Grain:
--   measurement_date
--   + table_id
--   + provider_code
-- =====================================================


with history_with_previous as (

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
        latest_update_date,

        -- =================================================
        -- Previous observation
        -- =================================================

        lag(measurement_date) over (
            partition by
                table_id,
                provider_code
            order by
                measurement_date
        ) as previous_measurement_date,

        lag(financial_year) over (
            partition by
                table_id,
                provider_code
            order by
                measurement_date
        ) as previous_financial_year,

        lag(financial_month) over (
            partition by
                table_id,
                provider_code
            order by
                measurement_date
        ) as previous_financial_month,

        lag(row_count) over (
            partition by
                table_id,
                provider_code
            order by
                measurement_date
        ) as previous_row_count,

        lag(activity_value) over (
            partition by
                table_id,
                provider_code
            order by
                measurement_date
        ) as previous_activity_value,

        lag(financial_value) over (
            partition by
                table_id,
                provider_code
            order by
                measurement_date
        ) as previous_financial_value,

        lag(null_activity_pct) over (
            partition by
                table_id,
                provider_code
            order by
                measurement_date
        ) as previous_null_activity_pct,

        lag(null_financial_pct) over (
            partition by
                table_id,
                provider_code
            order by
                measurement_date
        ) as previous_null_financial_pct

    from {{ ref('dq_monitoring_slam_history') }}

),


variance as (

    select
        *,

        -- =================================================
        -- Row count movement
        -- =================================================

        row_count
            - previous_row_count
            as row_count_change,

        round(
            100.0
            * (row_count - previous_row_count)
            / nullif(previous_row_count, 0),
            2
        ) as row_count_change_pct,


        -- =================================================
        -- Activity movement
        -- =================================================

        activity_value
            - previous_activity_value
            as activity_change,

        round(
            100.0
            * (activity_value - previous_activity_value)
            / nullif(previous_activity_value, 0),
            2
        ) as activity_change_pct,


        -- =================================================
        -- Financial movement
        -- =================================================

        financial_value
            - previous_financial_value
            as financial_change,

        round(
            100.0
            * (financial_value - previous_financial_value)
            / nullif(previous_financial_value, 0),
            2
        ) as financial_change_pct,


        -- =================================================
        -- NULL completeness movement
        --
        -- These are percentage-point changes.
        --
        -- Example:
        -- Previous NULL rate = 2%
        -- Current NULL rate  = 5%
        -- Change             = +3 percentage points
        -- =================================================

        round(
            null_activity_pct
            - previous_null_activity_pct,
            2
        ) as null_activity_pct_change,

        round(
            null_financial_pct
            - previous_null_financial_pct,
            2
        ) as null_financial_pct_change,


        -- =================================================
        -- Reporting period movement
        --
        -- Allows downstream DQ logic to distinguish:
        --
        -- 1. A same-period resubmission/change
        -- 2. Movement into a genuinely new FY/month
        -- =================================================

        case
            when previous_measurement_date is null
                then false

            when financial_year <> previous_financial_year
              or financial_month <> previous_financial_month
                then true

            else false
        end as reporting_period_changed

    from history_with_previous

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
    latest_update_date

from variance
