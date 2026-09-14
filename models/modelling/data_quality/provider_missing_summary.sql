--- provider_missing_summary.sql
--- Created by: J.Linney | dbt summary model for missing provider activity

--- Select the number of missing days per provider per dataset (APC, OP, ECDS)

-- 14/08/26 - JL - Added 4 NWL providers to the list of providers to monitor (in the files that feed this summary).  These are: RQM, R1K, RAS, RYJ.
--             which includes re-pointing the Organisation lookup to the new organisation_nhs_provider table to pick up the NWL names too.
--             and changed the action required message in this table to 'contact OneLondon' instead of ISL as that is now the responsible body.


{{
    config(
        materialized='table',
        tags='daily',
        post_hook=[
            "CALL MODELLING.DATA_QUALITY.SEND_PROVIDER_MISSING_SUMMARY()"
        ]
    )
}}

WITH 
apc_raw AS (
    SELECT PROVIDER, ACTIVITY_DATE, RECORDS
    FROM {{ ref('provider_daily_apc_activity') }}
    WHERE ACTIVITY_DATE >= DATEADD(day, -744, CURRENT_DATE)  -- 2 years monitoring window
    --AND ACTIVITY_DATE < DATEADD(day, -14, CURRENT_DATE)     -- Exclude last 2 weeks
),

op_raw AS (
    SELECT PROVIDER, ACTIVITY_DATE, RECORDS
    FROM {{ ref('provider_daily_op_activity') }}
    WHERE ACTIVITY_DATE >= DATEADD(day, -744, CURRENT_DATE)  -- 2 years monitoring window
    --AND ACTIVITY_DATE < DATEADD(day, -14, CURRENT_DATE)     -- Exclude last 2 weeks
),

ecds_raw AS (
    SELECT PROVIDER, ACTIVITY_DATE, RECORDS
    FROM {{ ref('provider_daily_ecds_activity') }}
    WHERE ACTIVITY_DATE >= DATEADD(day, -744, CURRENT_DATE)  -- 2 years monitoring window
    --AND ACTIVITY_DATE < DATEADD(day, -14, CURRENT_DATE)     -- Exclude last 2 weeks
),

-- Get all unique dates that appear in each dataset
apc_dates AS (SELECT DISTINCT ACTIVITY_DATE FROM apc_raw),
op_dates AS (SELECT DISTINCT ACTIVITY_DATE FROM op_raw),
ecds_dates AS (SELECT DISTINCT ACTIVITY_DATE FROM ecds_raw),

-- Get all unique providers that appear in each dataset
apc_providers AS (SELECT DISTINCT PROVIDER FROM apc_raw),
op_providers AS (SELECT DISTINCT PROVIDER FROM op_raw),
ecds_providers AS (SELECT DISTINCT PROVIDER FROM ecds_raw),

-- Create the expected grid (same as Python's Excel pivot structure)
-- Example for APC: all APC providers × all APC dates
apc_expected AS (
    SELECT p.PROVIDER, d.ACTIVITY_DATE, 'APC' AS dataset
    FROM apc_providers p
    CROSS JOIN apc_dates d
),

op_expected AS (
    SELECT p.PROVIDER, d.ACTIVITY_DATE, 'OP' AS dataset
    FROM op_providers p
    CROSS JOIN op_dates d
),

ecds_expected AS (
    SELECT p.PROVIDER, d.ACTIVITY_DATE, 'ECDS' AS dataset
    FROM ecds_providers p
    CROSS JOIN ecds_dates d
),

-- Find missing...
-- The logic is where provider submission is expected (ie. submissions have come in for other providers that day) 
--- so Day x Provider grid includes that date. Records = 0 is a future placeholder in case source is aggregated to 0 instead of being missing.
apc_missing AS (
    SELECT e.PROVIDER, e.ACTIVITY_DATE, e.dataset
    FROM apc_expected e
    LEFT JOIN apc_raw a 
        ON e.PROVIDER = a.PROVIDER 
        AND e.ACTIVITY_DATE = a.ACTIVITY_DATE
    WHERE a.ACTIVITY_DATE IS NULL OR a.RECORDS = 0
),

op_missing AS (
    SELECT e.PROVIDER, e.ACTIVITY_DATE, e.dataset
    FROM op_expected e
    LEFT JOIN op_raw o 
        ON e.PROVIDER = o.PROVIDER 
        AND e.ACTIVITY_DATE = o.ACTIVITY_DATE
    WHERE o.ACTIVITY_DATE IS NULL OR o.RECORDS = 0
),

ecds_missing AS (
    SELECT e.PROVIDER, e.ACTIVITY_DATE, e.dataset
    FROM ecds_expected e
    LEFT JOIN ecds_raw ec 
        ON e.PROVIDER = ec.PROVIDER 
        AND e.ACTIVITY_DATE = ec.ACTIVITY_DATE
    WHERE ec.ACTIVITY_DATE IS NULL OR ec.RECORDS = 0
),

all_missing AS (
    SELECT * FROM apc_missing
    UNION ALL
    SELECT * FROM op_missing
    UNION ALL
    SELECT * FROM ecds_missing
)

SELECT
    PROVIDER,
    COUNT(CASE WHEN dataset = 'APC' THEN 1 END) AS APC_MISSING_DAYS,
    COUNT(CASE WHEN dataset = 'OP' THEN 1 END) AS OP_MISSING_DAYS,
    COUNT(CASE WHEN dataset = 'ECDS' THEN 1 END) AS ECDS_MISSING_DAYS,
    COUNT(*) AS TOTAL_MISSING_SUBMISSIONS,
    'Contact OneLondon about missing submissions and notify users if critical' AS ACTION_REQUIRED
FROM all_missing
GROUP BY PROVIDER
ORDER BY PROVIDER