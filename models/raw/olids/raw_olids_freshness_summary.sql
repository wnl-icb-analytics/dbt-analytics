{{
    config(
        description="Raw layer (OLIDS stable layer - cleaned and filtered patient records). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.OLIDS.FRESHNESS_SUMMARY \ndbt: source(''olids'', ''FRESHNESS_SUMMARY'') \nColumns:\n  TABLE_NAME -> table_name\n  MAX_SOURCE_EXTRACTION_DATE -> max_source_extraction_date\n  MAX_LDS_TRANSFORM_DATETIME -> max_lds_transform_datetime\n  CONSENSUS_ACTIVITY_DATE -> consensus_activity_date\n  MAX_ACTIVITY_DATE -> max_activity_date\n  PRACTICES_REPORTING -> practices_reporting\n  PRACTICES_LAGGING_CONSENSUS_7D -> practices_lagging_consensus_7_d\n  PRACTICES_LAGGING_CONSENSUS_14D -> practices_lagging_consensus_14_d\n  WORST_PUBLISHER_CODE -> worst_publisher_code\n  WORST_PUBLISHER_LAG_DAYS -> worst_publisher_lag_days"
    )
}}
select
    "TABLE_NAME" as table_name,
    "MAX_SOURCE_EXTRACTION_DATE" as max_source_extraction_date,
    "MAX_LDS_TRANSFORM_DATETIME" as max_lds_transform_datetime,
    "CONSENSUS_ACTIVITY_DATE" as consensus_activity_date,
    "MAX_ACTIVITY_DATE" as max_activity_date,
    "PRACTICES_REPORTING" as practices_reporting,
    "PRACTICES_LAGGING_CONSENSUS_7D" as practices_lagging_consensus_7_d,
    "PRACTICES_LAGGING_CONSENSUS_14D" as practices_lagging_consensus_14_d,
    "WORST_PUBLISHER_CODE" as worst_publisher_code,
    "WORST_PUBLISHER_LAG_DAYS" as worst_publisher_lag_days
from {{ source('olids', 'FRESHNESS_SUMMARY') }}
