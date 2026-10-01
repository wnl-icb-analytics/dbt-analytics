{{
    config(
        description="Raw layer (OLIDS stable layer - cleaned and filtered patient records). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.OLIDS.NATIONAL_DATA_OPT_OUT \ndbt: source(''olids'', ''NATIONAL_DATA_OPT_OUT'') \nColumns:\n  LDS_BUSINESS_ID -> lds_business_id\n  LDS_RECORD_ID -> lds_record_id\n  SK_PATIENT_ID -> sk_patient_id\n  PREFERENCE_TYPE -> preference_type\n  PREFERENCE_STATUS -> preference_status\n  LDS_IS_DELETED -> lds_is_deleted\n  EFFECTIVE_FROM -> effective_from\n  EFFECTIVE_TO -> effective_to\n  IS_LATEST -> is_latest"
    )
}}
select
    "LDS_BUSINESS_ID" as lds_business_id,
    "LDS_RECORD_ID" as lds_record_id,
    "SK_PATIENT_ID" as sk_patient_id,
    "PREFERENCE_TYPE" as preference_type,
    "PREFERENCE_STATUS" as preference_status,
    "LDS_IS_DELETED" as lds_is_deleted,
    "EFFECTIVE_FROM" as effective_from,
    "EFFECTIVE_TO" as effective_to,
    "IS_LATEST" as is_latest
from {{ source('olids', 'NATIONAL_DATA_OPT_OUT') }}
