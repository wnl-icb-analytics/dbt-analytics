--- provider_daily_apc_activity.sql
--- Created by: J.Linney | dbt test file for APC provider daily activity

-- Add config overrides in here, eg. comment added to snowflake model metatadata
-- and materialisation type -  technically redundant here if declared in dbt_project.yml so just including it for clarity whilst testing
-- NOTE (18/12/25): Changed materialization now from view to table as missing_summary test running too early on blank views, reulting in it being empty
-- 14/08/26 - JL - Added 4 NWL providers to the list of providers to monitor.  These are: RQM, R1K, RAS, RYJ. 
--      which includes re-pointing the Organisation lookup to the new organisation_nhs_provider table to pick up the NWL names too.

{{
    config(
        materialized='table',
        tags='daily'
    )
}}

WITH base AS (
    SELECT
        "SPELL_COMMISSIONING_SERVICE_AGREEMENT_PROVIDER" AS provider_code, 
        DATE_TRUNC('day', spell_discharge_date) AS activity_date
    FROM {{ ref('stg_sus_apc_spell') }}
),
provider_lookup AS (
    SELECT 
        organisation_code,
        organisation_name
    from {{ ref('organisation_nhs_provider') }}
)
SELECT
    COALESCE(p.organisation_name, b.provider_code) AS provider,
    b.activity_date,
    COUNT(*) AS records
FROM base b
LEFT JOIN provider_lookup p ON b.provider_code = p.organisation_code
WHERE b.activity_date >= DATEADD(day, -744, CURRENT_DATE)  -- 2 years monitoring window
--AND b.activity_date < DATEADD(day, -14, CURRENT_DATE)      -- Exclude last 2 weeks
AND b.provider_code IN ('RRV', 'RKE', 'RAL','RP4','RP6','RAN','RYJ','RQM','R1K','RAS')
GROUP BY provider, activity_date