{{
    config(
        description="Raw layer (OLIDS stable layer - cleaned and filtered patient records). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.OLIDS.PATIENT_OPT_OUT \ndbt: source(''olids'', ''PATIENT_OPT_OUT'') \nColumns:\n  LDS_BUSINESS_ID -> lds_business_id\n  LDS_RECORD_ID -> lds_record_id\n  SK_PATIENT_ID -> sk_patient_id\n  PATIENT_INSTRUCTION_CATEGORY -> patient_instruction_category\n  PATIENT_INSTRUCTION_STATE -> patient_instruction_state\n  LDS_IS_DELETED -> lds_is_deleted\n  EFFECTIVE_FROM -> effective_from\n  EFFECTIVE_TO -> effective_to\n  IS_LATEST -> is_latest"
    )
}}
select
    "LDS_BUSINESS_ID" as lds_business_id,
    "LDS_RECORD_ID" as lds_record_id,
    "SK_PATIENT_ID" as sk_patient_id,
    "PATIENT_INSTRUCTION_CATEGORY" as patient_instruction_category,
    "PATIENT_INSTRUCTION_STATE" as patient_instruction_state,
    "LDS_IS_DELETED" as lds_is_deleted,
    "EFFECTIVE_FROM" as effective_from,
    "EFFECTIVE_TO" as effective_to,
    "IS_LATEST" as is_latest
from {{ source('olids', 'PATIENT_OPT_OUT') }}
