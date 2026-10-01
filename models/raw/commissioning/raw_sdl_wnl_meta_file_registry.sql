{{
    config(
        description="Raw layer (ServicesDataLocal canonical-named feeds, WNL footprint (SLAM contract monitoring)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SDL.META_FILE_REGISTRY \ndbt: source(''sdl_wnl'', ''META_FILE_REGISTRY'') \nColumns:\n  FEED -> feed\n  FILE_ID -> file_id\n  BATCH_ID -> batch_id\n  FILE_NAME -> file_name\n  ORIGINAL_FILE_NAME -> original_file_name\n  CREATED_DATETIME -> created_datetime\n  ROW_COUNT -> row_count\n  LOADER_JOB_NAME -> loader_job_name\n  LAST_REFRESHED_AT -> last_refreshed_at"
    )
}}
select
    "FEED" as feed,
    "FILE_ID" as file_id,
    "BATCH_ID" as batch_id,
    "FILE_NAME" as file_name,
    "ORIGINAL_FILE_NAME" as original_file_name,
    "CREATED_DATETIME" as created_datetime,
    "ROW_COUNT" as row_count,
    "LOADER_JOB_NAME" as loader_job_name,
    "LAST_REFRESHED_AT" as last_refreshed_at
from {{ source('sdl_wnl', 'META_FILE_REGISTRY') }}
