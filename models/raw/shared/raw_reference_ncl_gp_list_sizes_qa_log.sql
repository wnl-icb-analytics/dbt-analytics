{{
    config(
        description="Raw layer (Data management reference datasets). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.DATA_MANAGEMENT.NCL_GP_LIST_SIZES_QA_LOG \ndbt: source(''reference_data_management'', ''NCL_GP_LIST_SIZES_QA_LOG'') \nColumns:\n  RUN_DATE -> run_date\n  TOTAL_ROWS -> total_rows\n  NULL_ICB_COUNT -> null_icb_count\n  NULL_REGION_COUNT -> null_region_count\n  DUPLICATE_PRACTICE_COUNT -> duplicate_practice_count\n  ORPHANED_SUB_ICB_COUNT -> orphaned_sub_icb_count"
    )
}}
select
    "RUN_DATE" as run_date,
    "TOTAL_ROWS" as total_rows,
    "NULL_ICB_COUNT" as null_icb_count,
    "NULL_REGION_COUNT" as null_region_count,
    "DUPLICATE_PRACTICE_COUNT" as duplicate_practice_count,
    "ORPHANED_SUB_ICB_COUNT" as orphaned_sub_icb_count
from {{ source('reference_data_management', 'NCL_GP_LIST_SIZES_QA_LOG') }}
