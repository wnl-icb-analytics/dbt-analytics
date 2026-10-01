{{
    config(
        description="Raw layer (ServicesDataLocal canonical-named feeds, WNL footprint (SLAM contract monitoring)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SDL.META_UNMAPPED_FILES \ndbt: source(''sdl_wnl'', ''META_UNMAPPED_FILES'') \nColumns:\n  FEED -> feed\n  FILE_ID -> file_id\n  BATCH_ID -> batch_id\n  HEADER_ID -> header_id\n  PROFILE_CODE -> profile_code\n  ORIGINAL_FILE_NAME -> original_file_name\n  ROW_COUNT -> row_count\n  LOGGED_AT -> logged_at\n  LOADER_JOB_NAME -> loader_job_name\n  HAS_HEADERS -> has_headers\n  EXCEPTION_DETECTED_AT -> exception_detected_at\n  REASON -> reason"
    )
}}
select
    "FEED" as feed,
    "FILE_ID" as file_id,
    "BATCH_ID" as batch_id,
    "HEADER_ID" as header_id,
    "PROFILE_CODE" as profile_code,
    "ORIGINAL_FILE_NAME" as original_file_name,
    "ROW_COUNT" as row_count,
    "LOGGED_AT" as logged_at,
    "LOADER_JOB_NAME" as loader_job_name,
    "HAS_HEADERS" as has_headers,
    "EXCEPTION_DETECTED_AT" as exception_detected_at,
    "REASON" as reason
from {{ source('sdl_wnl', 'META_UNMAPPED_FILES') }}
