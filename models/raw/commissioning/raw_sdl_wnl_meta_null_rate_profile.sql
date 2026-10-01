{{
    config(
        description="Raw layer (ServicesDataLocal canonical-named feeds, WNL footprint (SLAM contract monitoring)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SDL.META_NULL_RATE_PROFILE \ndbt: source(''sdl_wnl'', ''META_NULL_RATE_PROFILE'') \nColumns:\n  FEED -> feed\n  TABLE_NAME -> table_name\n  COLUMN_NAME -> column_name\n  ORDINAL_POSITION -> ordinal_position\n  TOTAL_ROWS -> total_rows\n  NON_NULL_ROWS -> non_null_rows\n  NULL_ROWS -> null_rows\n  NULL_RATE -> null_rate\n  IS_ALL_NULL -> is_all_null\n  COMPUTED_AT -> computed_at"
    )
}}
select
    "FEED" as feed,
    "TABLE_NAME" as table_name,
    "COLUMN_NAME" as column_name,
    "ORDINAL_POSITION" as ordinal_position,
    "TOTAL_ROWS" as total_rows,
    "NON_NULL_ROWS" as non_null_rows,
    "NULL_ROWS" as null_rows,
    "NULL_RATE" as null_rate,
    "IS_ALL_NULL" as is_all_null,
    "COMPUTED_AT" as computed_at
from {{ source('sdl_wnl', 'META_NULL_RATE_PROFILE') }}
