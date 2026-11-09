{{
    config(
        materialized='view',
        tags=['quick_reports'])
}}
--select latest source received at date
select MAX(DATE(source_received_at)) AS sus_refresh_date
FROM {{ ref("stg_sus_ecds_emergency_care") }}
--FROM STAGING.SUS.STG_SUS_ECDS_EMERGENCY_CARE