/*
  Model Spill Report

  Models ranked by bytes spilled during their last successful build.
  Spilling means a sort/window/join intermediate exceeded warehouse memory;
  every spilled GB is written and re-read mid-query, multiplying execution
  time. Remote spill (cloud storage) is far slower than local (SSD).

  spill_ratio = spilled / scanned. A ratio well above 1 means an operator
  inflates the data far beyond its input - usually a wide column set dragged
  through a window function or sort. Fixes, in order:
  1. narrow the columns entering the sort/window/dedup, rejoin payload after
  2. pre-aggregate or split the heavy operator
  3. route the model to a larger warehouse (snowflake_warehouse config)

  Caveats:
  - SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY lags up to 45 minutes and needs an
    elevated role (run with DBT_ADMIN / DATA_PLATFORM_MANAGER)

  Run via: dbt compile then execute target/.../model_spill_report.sql,
  or paste directly into snow sql / a worksheet (plain SQL, no Jinja).
*/

WITH model_queries AS (
    SELECT
        -- wnl_analytics only: excludes upstream dbt-olids (stable_*) builds
        REGEXP_SUBSTR(query_text, '"node_id":\\s*"model\\.wnl_analytics\\.([^"]+)"', 1, 1, 'e', 1) AS model_name,
        warehouse_name,
        start_time,
        execution_time / 1000 AS exec_secs,
        bytes_scanned / 1e9 AS gb_scanned,
        bytes_spilled_to_local_storage / 1e9 AS gb_spilled_local,
        bytes_spilled_to_remote_storage / 1e9 AS gb_spilled_remote
    FROM snowflake.account_usage.query_history
    WHERE start_time > DATEADD('day', -30, CURRENT_TIMESTAMP)
        AND query_type IN ('CREATE_TABLE_AS_SELECT', 'INSERT', 'MERGE', 'CREATE_VIEW')
        AND execution_status = 'SUCCESS'
),

last_runs AS (
    SELECT *
    FROM model_queries
    WHERE model_name IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY model_name ORDER BY start_time DESC) = 1
)

SELECT
    model_name,
    warehouse_name,
    start_time AS last_success_at,
    ROUND(exec_secs, 1) AS exec_secs,
    ROUND(gb_scanned, 2) AS gb_scanned,
    ROUND(gb_spilled_local, 2) AS gb_spilled_local,
    ROUND(gb_spilled_remote, 2) AS gb_spilled_remote,
    ROUND(gb_spilled_local + gb_spilled_remote, 2) AS gb_spilled_total,
    ROUND((gb_spilled_local + gb_spilled_remote) / NULLIF(gb_scanned, 0), 1) AS spill_ratio,
    CASE
        WHEN gb_spilled_remote > 1
            THEN 'Remote spill - severe memory pressure, fix or upsize warehouse'
        WHEN (gb_spilled_local + gb_spilled_remote) / NULLIF(gb_scanned, 0) > 3
            THEN 'Intermediate blow-up - narrow columns entering sort/window'
        WHEN gb_spilled_local + gb_spilled_remote > 5
            THEN 'Heavy local spill - review sort/window width'
        ELSE 'Minor'
    END AS assessment
FROM last_runs
WHERE gb_spilled_local + gb_spilled_remote > 0.1
ORDER BY gb_spilled_total DESC
