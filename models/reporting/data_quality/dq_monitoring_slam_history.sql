{{
    config(
        materialized='incremental',
        unique_key=[
            'measurement_date',
            'table_id',
            'provider_code'
        ]
    )
}}

-- =====================================================
-- SLAM DQ Monitoring History
--
-- Preserves one daily snapshot of the enriched SLAM
-- metrics for each configured core provider and feed.
--
-- Grain:
--   measurement_date
--   + table_id
--   + provider_code
-- =====================================================

with measurements as (

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

    from {{ ref('dq_monitoring_slam_metrics') }}

)

--- Below code guards against further incremental additions if the row already exists in the target table. 
--- For same-day re-runs, the latest snapshot will be preserved in the metric ("this is what slam looks like right now") table

select *
from measurements

{% if is_incremental() %}

where not exists (

    select 1
    from {{ this }} existing

    where existing.measurement_date
              = measurements.measurement_date

      and existing.table_id
              = measurements.table_id

      and existing.provider_code
              = measurements.provider_code

)

{% endif %}
