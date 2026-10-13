{{
    config(
        description="Raw layer: Cached SNOMED CT expansion of Expression Constraint Language (ECL) definitions, one row per cluster ID and code, with refresh metadata. 1:1 passthrough with cleaned column names. \nSource: APPLICATIONS.SNOMED_CLUSTER_MANAGER.ECL_CACHE \ndbt: source(''snomed_cluster_manager'', ''ECL_CACHE'') \nColumns:\n  CLUSTER_ID -> cluster_id\n  CODE -> code\n  DISPLAY -> display\n  SYSTEM -> system\n  LAST_REFRESHED -> last_refreshed\n  ECL_EXPRESSION_HASH -> ecl_expression_hash"
    )
}}
select
    "CLUSTER_ID" as cluster_id,
    "CODE" as code,
    "DISPLAY" as display,
    "SYSTEM" as system,
    "LAST_REFRESHED" as last_refreshed,
    "ECL_EXPRESSION_HASH" as ecl_expression_hash
from {{ source('snomed_cluster_manager', 'ECL_CACHE') }}
