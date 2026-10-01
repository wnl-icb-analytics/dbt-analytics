{{
    config(
        description="Raw layer (ServicesDataLocal canonical-named feeds, WNL footprint (SLAM contract monitoring)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SDL.META_ROW_COUNT_COMPARE \ndbt: source(''sdl_wnl'', ''META_ROW_COUNT_COMPARE'') \nColumns:\n  FEED -> feed\n  SOURCE_ROWS -> source_rows\n  FINAL_ROWS -> final_rows\n  GAP -> gap\n  COVERAGE_PCT -> coverage_pct"
    )
}}
select
    "FEED" as feed,
    "SOURCE_ROWS" as source_rows,
    "FINAL_ROWS" as final_rows,
    "GAP" as gap,
    "COVERAGE_PCT" as coverage_pct
from {{ source('sdl_wnl', 'META_ROW_COUNT_COMPARE') }}
