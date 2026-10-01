/*
    Preview Orphaned dbt Objects

    Preflight check to see which objects would be dropped by CLEANUP_ORPHANED_DBT_OBJECTS.
    Run this before executing the cleanup procedure to verify the results.
*/

WITH snowflake_objects AS (
    SELECT
        table_catalog,
        table_schema,
        table_name,
        table_type,
        comment,
        last_altered,
        DATEDIFF(day, last_altered, CURRENT_DATE) AS days_since_altered
    FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
    WHERE comment LIKE '%🤖%'
      AND (comment LIKE '%github.com/ncl-icb-analytics/dbt-ncl-analytics%'
        OR comment LIKE '%github.com/wnl-icb-analytics/dbt-analytics%')
      AND table_catalog IN (
        'STAGING',
        'MODELLING',
        'REPORTING',
        'REFERENCE',
        'DEV__REFERENCE',
        'PUBLISHED_REPORTING__DIRECT_CARE',
        'PUBLISHED_REPORTING__SECONDARY_USE',
        'DEV__STAGING',
        'DEV__MODELLING',
        'DEV__REPORTING',
        'DEV__PUBLISHED_REPORTING__DIRECT_CARE',
        'DEV__PUBLISHED_REPORTING__SECONDARY_USE'
      )
      AND deleted IS NULL
),
current_dbt_models AS (
    SELECT
        UPPER(database_name) AS database_name,
        UPPER(schema_name) AS schema_name,
        UPPER(alias) AS table_name
    FROM DATA_LAKE__NCL.DBT_OBSERVABILITY.DBT_MODELS
    WHERE package_name = 'wnl_analytics'
    UNION ALL
    SELECT
        'DEV__' || UPPER(database_name) AS database_name,
        UPPER(schema_name) AS schema_name,
        UPPER(alias) AS table_name
    FROM DATA_LAKE__NCL.DBT_OBSERVABILITY.DBT_MODELS
    WHERE package_name = 'wnl_analytics'
)
SELECT
    s.table_catalog,
    s.table_schema,
    s.table_name,
    s.table_type,
    s.last_altered,
    s.days_since_altered,
    s.comment
FROM snowflake_objects s
LEFT JOIN current_dbt_models m
    ON s.table_catalog = m.database_name
   AND s.table_schema = m.schema_name
   AND s.table_name = m.table_name
WHERE m.table_name IS NULL
  AND s.days_since_altered > 35
ORDER BY s.table_catalog, s.table_schema, s.table_name;
