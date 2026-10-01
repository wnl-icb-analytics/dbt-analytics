{{
    config(
        description="Raw layer (OLIDS stable layer - cleaned and filtered patient records). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.OLIDS.FRESHNESS \ndbt: source(''olids'', ''FRESHNESS'') \nColumns:\n  TABLE_NAME -> table_name\n  PUBLISHER_CODE -> publisher_code\n  MAX_SOURCE_EXTRACTION_DATE -> max_source_extraction_date\n  MAX_LDS_TRANSFORM_DATETIME -> max_lds_transform_datetime\n  MAX_ACTIVITY_DATE -> max_activity_date\n  CONSENSUS_ACTIVITY_DATE -> consensus_activity_date\n  ACTIVITY_LAG_DAYS -> activity_lag_days"
    )
}}
select
    "TABLE_NAME" as table_name,
    "PUBLISHER_CODE" as publisher_code,
    "MAX_SOURCE_EXTRACTION_DATE" as max_source_extraction_date,
    "MAX_LDS_TRANSFORM_DATETIME" as max_lds_transform_datetime,
    "MAX_ACTIVITY_DATE" as max_activity_date,
    "CONSENSUS_ACTIVITY_DATE" as consensus_activity_date,
    "ACTIVITY_LAG_DAYS" as activity_lag_days
from {{ source('olids', 'FRESHNESS') }}
