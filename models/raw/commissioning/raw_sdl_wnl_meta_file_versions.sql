{{
    config(
        description="Raw layer (ServicesDataLocal canonical-named feeds, WNL footprint (SLAM contract monitoring)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SDL.META_FILE_VERSIONS \ndbt: source(''sdl_wnl'', ''META_FILE_VERSIONS'') \nColumns:\n  FEED -> feed\n  FILE_ID -> file_id\n  BATCH_ID -> batch_id\n  VERSION_ID -> version_id\n  MATCH_METHOD -> match_method"
    )
}}
select
    "FEED" as feed,
    "FILE_ID" as file_id,
    "BATCH_ID" as batch_id,
    "VERSION_ID" as version_id,
    "MATCH_METHOD" as match_method
from {{ source('sdl_wnl', 'META_FILE_VERSIONS') }}
