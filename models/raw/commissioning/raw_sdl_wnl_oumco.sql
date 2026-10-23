{{
    config(
        description="Raw layer (ServicesDataLocal canonical-named feeds, WNL footprint). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SDL.OUMCO \ndbt: source(''sdl_wnl'', ''OUMCO'') \nColumns:\n  META_SK_ROW_ID -> meta_sk_row_id\n  META_FILE_ID -> meta_file_id\n  META_ROW_ID -> meta_row_id\n  META_BATCH_ID -> meta_batch_id\n  META_PARTITION_DATE -> meta_partition_date\n  META_PROVIDER_CODE -> meta_provider_code\n  META_RECIPIENT_CODE -> meta_recipient_code\n  META_VERSION_ID -> meta_version_id"
    )
}}
select
    "META_SK_ROW_ID" as meta_sk_row_id,
    "META_FILE_ID" as meta_file_id,
    "META_ROW_ID" as meta_row_id,
    "META_BATCH_ID" as meta_batch_id,
    "META_PARTITION_DATE" as meta_partition_date,
    "META_PROVIDER_CODE" as meta_provider_code,
    "META_RECIPIENT_CODE" as meta_recipient_code,
    "META_VERSION_ID" as meta_version_id
from {{ source('sdl_wnl', 'OUMCO') }}
