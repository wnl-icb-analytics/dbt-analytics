{{
    config(
        description="Raw layer (ServicesDataLocal canonical-named feeds, WNL footprint (SLAM contract monitoring)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SDL.META_REFRESH_LOG \ndbt: source(''sdl_wnl'', ''META_REFRESH_LOG'') \nColumns:\n  RUN_ID -> run_id\n  FEED -> feed\n  STARTED_AT -> started_at\n  COMPLETED_AT -> completed_at\n  NEW_FILES_MATCHED -> new_files_matched\n  NEW_EXCEPTIONS -> new_exceptions\n  TOTAL_MAPPED_FILES -> total_mapped_files\n  TOTAL_EXCEPTIONS -> total_exceptions\n  STATUS -> status\n  ERROR_MESSAGE -> error_message"
    )
}}
select
    "RUN_ID" as run_id,
    "FEED" as feed,
    "STARTED_AT" as started_at,
    "COMPLETED_AT" as completed_at,
    "NEW_FILES_MATCHED" as new_files_matched,
    "NEW_EXCEPTIONS" as new_exceptions,
    "TOTAL_MAPPED_FILES" as total_mapped_files,
    "TOTAL_EXCEPTIONS" as total_exceptions,
    "STATUS" as status,
    "ERROR_MESSAGE" as error_message
from {{ source('sdl_wnl', 'META_REFRESH_LOG') }}
