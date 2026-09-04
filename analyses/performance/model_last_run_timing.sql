/*
  Model Last Run Timing

  Last successful build query per dbt model (attributed via the node_id in
  the dbt query comment), with elapsed time split into:
  - queued_secs:    warehouse queueing (overload + provisioning + repair)
  - blocked_secs:   waiting on transaction locks
  - compile_secs:   Snowflake query compilation
  - exec_secs:      actual execution on the warehouse

  High queued_secs with modest exec_secs = warehouse contention (concurrency),
  not a slow query - fix scheduling/warehouse size, not the model.

  Caveats:
  - SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY lags up to 45 minutes and needs an
    elevated role (run with DBT_ADMIN / DATA_PLATFORM_MANAGER)
  - views appear with their (trivial) CREATE VIEW statement; tables and
    incrementals with CTAS / INSERT / MERGE

  Run via: dbt compile then execute target/.../model_last_run_timing.sql,
  or paste directly into snow sql / a worksheet (plain SQL, no Jinja).
*/

WITH model_queries AS (
    SELECT
        -- wnl_analytics only: excludes upstream dbt-olids (stable_*) builds
        REGEXP_SUBSTR(query_text, '"node_id":\\s*"model\\.wnl_analytics\\.([^"]+)"', 1, 1, 'e', 1) AS model_name,
        query_type,
        warehouse_name,
        start_time,
        total_elapsed_time / 1000 AS total_secs,
        (queued_overload_time + queued_provisioning_time + queued_repair_time) / 1000 AS queued_secs,
        transaction_blocked_time / 1000 AS blocked_secs,
        compilation_time / 1000 AS compile_secs,
        execution_time / 1000 AS exec_secs,
        partitions_scanned,
        partitions_total,
        bytes_scanned / 1e9 AS gb_scanned
    FROM snowflake.account_usage.query_history
    WHERE start_time > DATEADD('day', -30, CURRENT_TIMESTAMP)
        AND query_type IN ('CREATE_TABLE_AS_SELECT', 'INSERT', 'MERGE', 'CREATE_VIEW')
        AND execution_status = 'SUCCESS'
)

SELECT
    model_name,
    query_type,
    warehouse_name,
    start_time AS last_success_at,
    ROUND(total_secs, 1) AS total_secs,
    ROUND(queued_secs, 1) AS queued_secs,
    ROUND(blocked_secs, 1) AS blocked_secs,
    ROUND(compile_secs, 1) AS compile_secs,
    ROUND(exec_secs, 1) AS exec_secs,
    ROUND(queued_secs / NULLIF(total_secs, 0) * 100, 0) AS pct_queued,
    partitions_scanned,
    partitions_total,
    ROUND(gb_scanned, 2) AS gb_scanned,
    CASE
        WHEN queued_secs > exec_secs AND queued_secs > 30
            THEN 'Mostly queueing - warehouse contention, not a slow query'
        WHEN exec_secs > 300 THEN 'Slow execution (>5min) - optimise query'
        WHEN exec_secs > 120 THEN 'Moderate execution (>2min)'
        ELSE 'OK'
    END AS assessment
FROM model_queries
WHERE model_name IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY model_name ORDER BY start_time DESC) = 1
ORDER BY total_secs DESC
