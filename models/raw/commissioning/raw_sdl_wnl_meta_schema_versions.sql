{{
    config(
        description="Raw layer (ServicesDataLocal canonical-named feeds, WNL footprint (SLAM contract monitoring)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SDL.META_SCHEMA_VERSIONS \ndbt: source(''sdl_wnl'', ''META_SCHEMA_VERSIONS'') \nColumns:\n  FEED -> feed\n  VERSION_ID -> version_id\n  FILE_COUNT -> file_count\n  FIRST_SEEN -> first_seen\n  LAST_SEEN -> last_seen\n  RAW_MAPPING -> raw_mapping\n  COLUMN_MAPPING -> column_mapping"
    )
}}
select
    "FEED" as feed,
    "VERSION_ID" as version_id,
    "FILE_COUNT" as file_count,
    "FIRST_SEEN" as first_seen,
    "LAST_SEEN" as last_seen,
    "RAW_MAPPING" as raw_mapping,
    "COLUMN_MAPPING" as column_mapping
from {{ source('sdl_wnl', 'META_SCHEMA_VERSIONS') }}
