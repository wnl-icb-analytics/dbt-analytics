{{
    config(
        description="Raw layer (ServicesDataLocal canonical-named feeds, WNL footprint (SLAM contract monitoring)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SDL.META_EXCEPTIONS \ndbt: source(''sdl_wnl'', ''META_EXCEPTIONS'') \nColumns:\n  FEED -> feed\n  FILE_ID -> file_id\n  BATCH_ID -> batch_id\n  SOURCE_COLUMN_HEADERS -> source_column_headers\n  DETECTED_AT -> detected_at\n  RESOLVED -> resolved"
    )
}}
select
    "FEED" as feed,
    "FILE_ID" as file_id,
    "BATCH_ID" as batch_id,
    "SOURCE_COLUMN_HEADERS" as source_column_headers,
    "DETECTED_AT" as detected_at,
    "RESOLVED" as resolved
from {{ source('sdl_wnl', 'META_EXCEPTIONS') }}
