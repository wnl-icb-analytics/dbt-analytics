{{
    config(
        description="Raw layer (ServicesDataLocal canonical-named feeds, WNL footprint (SLAM contract monitoring)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SDL.META_BUILD_STATE \ndbt: source(''sdl_wnl'', ''META_BUILD_STATE'') \nColumns:\n  FEED -> feed\n  MAX_FILE_ID_LOADED -> max_file_id_loaded\n  LAST_BUILT_AT -> last_built_at\n  LAST_BULK_AT -> last_bulk_at\n  NEEDS_REBUILD -> needs_rebuild\n  LAST_ERROR -> last_error"
    )
}}
select
    "FEED" as feed,
    "MAX_FILE_ID_LOADED" as max_file_id_loaded,
    "LAST_BUILT_AT" as last_built_at,
    "LAST_BULK_AT" as last_bulk_at,
    "NEEDS_REBUILD" as needs_rebuild,
    "LAST_ERROR" as last_error
from {{ source('sdl_wnl', 'META_BUILD_STATE') }}
