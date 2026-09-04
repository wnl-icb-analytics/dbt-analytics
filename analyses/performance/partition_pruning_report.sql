/*
  Partition Pruning Report

  Per-model micro-partition pruning stats from Snowflake query history,
  attributed via the dbt query comment (node_id) appended to every query.

  pct_scanned reads:
  - low  = predicates/joins hit the source table cluster keys (good pruning)
  - high = full scan; fine for whole-table builds, a problem when the model
    selects a small slice (check filters against the source cluster keys,
    e.g. OLIDS medication tables: bnf_chapter, mapped_concept_code,
    clinical_effective_date)

  Caveats:
  - partitions_total counts all tables referenced by the query, so pct is a
    blended figure, not per-table
  - small scans (< 100 partitions) are excluded: pruning is irrelevant there
  - SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY lags up to 45 minutes and needs an
    elevated role (run with DBT_ADMIN / DATA_PLATFORM_MANAGER)

  Run via: dbt compile then execute target/.../partition_pruning_report.sql,
  or paste directly into snow sql / a worksheet (plain SQL, no Jinja).
*/

WITH model_queries AS (
    SELECT
        -- dbt appends: /* {"app": "dbt", ..., "node_id": "model.wnl_analytics.<name>", ...} */
        -- wnl_analytics only: excludes upstream dbt-olids (stable_*) builds
        REGEXP_SUBSTR(query_text, '"node_id":\\s*"model\\.wnl_analytics\\.([^"]+)"', 1, 1, 'e', 1) AS model_name,
        query_type,
        start_time,
        total_elapsed_time / 1000 AS elapsed_secs,
        partitions_scanned,
        partitions_total,
        bytes_scanned / 1e9 AS gb_scanned
    FROM snowflake.account_usage.query_history
    WHERE start_time > DATEADD('day', -7, CURRENT_TIMESTAMP)
        AND query_type IN ('CREATE_TABLE_AS_SELECT', 'INSERT', 'MERGE')
        AND partitions_total >= 100
        AND execution_status = 'SUCCESS'
),

per_model AS (
    SELECT
        model_name,
        COUNT(*) AS runs_7d,
        ROUND(AVG(elapsed_secs), 1) AS avg_secs,
        ROUND(AVG(partitions_scanned), 0) AS avg_partitions_scanned,
        ROUND(AVG(partitions_total), 0) AS avg_partitions_total,
        ROUND(AVG(partitions_scanned / partitions_total) * 100, 1) AS avg_pct_scanned,
        ROUND(AVG(gb_scanned), 2) AS avg_gb_scanned,
        ROUND(SUM(gb_scanned), 1) AS total_gb_7d
    FROM model_queries
    WHERE model_name IS NOT NULL
    GROUP BY model_name
)

SELECT
    model_name,
    runs_7d,
    avg_secs,
    avg_partitions_scanned,
    avg_partitions_total,
    avg_pct_scanned,
    avg_gb_scanned,
    total_gb_7d,
    CASE
        WHEN avg_pct_scanned <= 15 THEN '✅ Excellent'
        WHEN avg_pct_scanned <= 40 THEN '🟢 Good'
        WHEN avg_pct_scanned <= 70 THEN '🟡 Partial'
        ELSE '🔴 Full scan'
    END AS pruning,
    CASE
        WHEN avg_pct_scanned > 70 AND total_gb_7d > 20
            THEN 'High cost + no pruning - check filters vs source cluster keys'
        WHEN avg_pct_scanned > 70
            THEN 'Full scan - fine if whole-table build'
        WHEN avg_pct_scanned > 40 AND total_gb_7d > 20
            THEN 'Partial pruning on a heavy scan - review predicates'
        ELSE 'OK'
    END AS suggestion
FROM per_model
ORDER BY total_gb_7d DESC
