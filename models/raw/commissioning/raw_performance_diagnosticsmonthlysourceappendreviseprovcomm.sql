{{
    config(
        description="Raw layer (Current Performance presentation feeds). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.PERFORMANCE.DiagnosticsMonthlySourceAppendReviseProvComm \ndbt: source(''performance'', ''DiagnosticsMonthlySourceAppendReviseProvComm'') \nColumns:\n  PERIOD -> period\n  _INGESTED_AT -> ingested_at"
    )
}}
select
    "PERIOD" as period,
    "_INGESTED_AT" as ingested_at
from {{ source('performance', 'DiagnosticsMonthlySourceAppendReviseProvComm') }}
