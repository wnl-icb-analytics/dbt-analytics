{{
    config(
        description="Raw layer (C-LTCS tables). 1:1 passthrough with cleaned column names. \nSource: {{ 'PUBLISHED_REPORTING__DIRECT_CARE' if target.name in ['prod', 'snowflake-prod', 'ci-prod'] else ('DEV__PUBLISHED_REPORTING__DIRECT_CARE' if target.name in ['dev', 'snowflake-dev', 'ci-dev'] else target.name | upper | trim ~ '__PUBLISHED_REPORTING__DIRECT_CARE') }}.C_LTCS.STATUS_LOG \ndbt: source(''c_ltcs'', ''STATUS_LOG'') \nColumns:\n  PATIENT_ID -> patient_id\n  AREA_CODE -> area_code\n  INTERVENTION_DATE -> intervention_date\n  ACTION -> action\n  ACTION_DATE -> action_date\n  DETAIL -> detail\n  INTERVENTION_NAME -> intervention_name\n  STREAM_ACTION -> stream_action\n  IS_UPDATE -> is_update\n  REPLICATED_AT -> replicated_at\n  SOURCE_ROW_ID -> source_row_id"
    )
}}
select
    "PATIENT_ID" as patient_id,
    "AREA_CODE" as area_code,
    "INTERVENTION_DATE" as intervention_date,
    "ACTION" as action,
    "ACTION_DATE" as action_date,
    "DETAIL" as detail,
    "INTERVENTION_NAME" as intervention_name,
    "STREAM_ACTION" as stream_action,
    "IS_UPDATE" as is_update,
    "REPLICATED_AT" as replicated_at,
    "SOURCE_ROW_ID" as source_row_id
from {{ source('c_ltcs', 'STATUS_LOG') }}
