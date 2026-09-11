{{
    config(
        description="Raw layer (SNOMED CT cross-map and terminology reference data. Sourced by NHS TRUD.). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.TERMINOLOGY.INGEST_LOG \ndbt: source(''reference_trud_terminology'', ''INGEST_LOG'') \nColumns:\n  RUN_ID -> run_id\n  RUN_START -> run_start\n  RUN_END -> run_end\n  STATUS -> status\n  FORCE_RELOAD -> force_reload\n  RELEASE_ID -> release_id\n  RELEASE_DATE -> release_date\n  ROWS_STAGED -> rows_staged\n  ROWS_LATEST -> rows_latest\n  ROWS_ARCHIVED -> rows_archived\n  DURATION_SECONDS -> duration_seconds\n  ERROR_MESSAGE -> error_message"
    )
}}
select
    "RUN_ID" as run_id,
    "RUN_START" as run_start,
    "RUN_END" as run_end,
    "STATUS" as status,
    "FORCE_RELOAD" as force_reload,
    "RELEASE_ID" as release_id,
    "RELEASE_DATE" as release_date,
    "ROWS_STAGED" as rows_staged,
    "ROWS_LATEST" as rows_latest,
    "ROWS_ARCHIVED" as rows_archived,
    "DURATION_SECONDS" as duration_seconds,
    "ERROR_MESSAGE" as error_message
from {{ source('reference_trud_terminology', 'INGEST_LOG') }}
