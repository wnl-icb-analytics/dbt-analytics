{{
    config(
        description="Raw layer (Data management reference datasets). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.DATA_MANAGEMENT.NCL_GP_PRACTICE_LATEST_LIST_SIZES_BACKUP2 \ndbt: source(''reference_data_management'', ''NCL_GP_PRACTICE_LATEST_LIST_SIZES_BACKUP2'') \nColumns:\n  LIST_SIZE_VALUE -> list_size_value\n  LIST_SIZE_DATE -> list_size_date\n  SOURCE_ID -> source_id\n  SOURCE_SHORT_NAME -> source_short_name\n  SOURCE_NAME -> source_name\n  GP_PRACTICE_CODE -> gp_practice_code\n  GP_PRACTICE_NAME -> gp_practice_name\n  ORGANISATION_CODE -> organisation_code\n  SUB_ICB_CODE -> sub_icb_code"
    )
}}
select
    "LIST_SIZE_VALUE" as list_size_value,
    "LIST_SIZE_DATE" as list_size_date,
    "SOURCE_ID" as source_id,
    "SOURCE_SHORT_NAME" as source_short_name,
    "SOURCE_NAME" as source_name,
    "GP_PRACTICE_CODE" as gp_practice_code,
    "GP_PRACTICE_NAME" as gp_practice_name,
    "ORGANISATION_CODE" as organisation_code,
    "SUB_ICB_CODE" as sub_icb_code
from {{ source('reference_data_management', 'NCL_GP_PRACTICE_LATEST_LIST_SIZES_BACKUP2') }}
