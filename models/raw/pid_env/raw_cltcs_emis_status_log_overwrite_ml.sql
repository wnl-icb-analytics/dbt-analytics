{{
    config(
        description="Raw layer (EMIS extract for c-ltcs pipeline from PID environment via MESH and other local lookups). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.CLTCS.STATUS_LOG_OVERWRITE_ML \ndbt: source(''cltcs_emis_extract'', ''STATUS_LOG_OVERWRITE_ML'') \nColumns:\n  SK_PATIENT_ID -> sk_patient_id\n  NEIGHBOURHOOD -> neighbourhood\n  STATUS -> status\n  INTERVENTION_DATE -> intervention_date\n  LIST_NAME -> list_name\n  INTERVENTION_NAME -> intervention_name\n  CLINICAL -> clinical\n  IMPACTIBLE -> impactible\n  REASON -> reason"
    )
}}
select
    "SK_PATIENT_ID" as sk_patient_id,
    "NEIGHBOURHOOD" as neighbourhood,
    "STATUS" as status,
    "INTERVENTION_DATE" as intervention_date,
    "LIST_NAME" as list_name,
    "INTERVENTION_NAME" as intervention_name,
    "CLINICAL" as clinical,
    "IMPACTIBLE" as impactible,
    "REASON" as reason
from {{ source('cltcs_emis_extract', 'STATUS_LOG_OVERWRITE_ML') }}
