{{ config(
    materialized='view'
) }}

-- =====================================================
-- SLAM DQ Monitoring - Current
--
-- Returns the latest available SLAM DQ status for each
-- configured provider and feed.
--
-- Source:
--   dq_monitoring_slam_status
--
-- Grain:
--   table_id
--   + provider_code
--
-- This model contains no additional DQ logic.
-- It provides a simple current-state layer for
-- downstream reporting and Streamlit.
-- =====================================================


with ranked_status as (

    select
        *,

        row_number() over (
            partition by
                table_id,
                provider_code
            order by
                measurement_date desc,
                measured_at desc
        ) as current_row_number

    from {{ ref('dq_monitoring_slam_status') }}

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

from ranked_status

where current_row_number = 1
