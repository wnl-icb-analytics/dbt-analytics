{{
    config(
        description="Raw layer (Curated primary care organisation reference - practices, PCNs, neighbourhoods and memberships). 1:1 passthrough with cleaned column names. \nSource: REFERENCE.PRIMARY_CARE.PCN_HISTORY \ndbt: source(''reference_primary_care'', ''PCN_HISTORY'') \nColumns:\n  PCN_CODE -> pcn_code\n  PCN_NAME -> pcn_name\n  PCN_NAME_WITH_BOROUGH -> pcn_name_with_borough\n  SUB_ICB_CODE -> sub_icb_code\n  REGISTERED_BOROUGH_NAME -> registered_borough_name\n  OPEN_DATE -> open_date\n  CLOSE_DATE -> close_date\n  PCN_STATUS -> pcn_status\n  VALID_FROM -> valid_from\n  VALID_TO -> valid_to\n  IS_CURRENT -> is_current\n  SOURCE_SYSTEM -> source_system\n  CHANGE_REASON -> change_reason\n  CREATED_BY -> created_by\n  CREATED_AT -> created_at\n  HEALTH_BOROUGH_NAME -> health_borough_name"
    )
}}
select
    "PCN_CODE" as pcn_code,
    "PCN_NAME" as pcn_name,
    "PCN_NAME_WITH_BOROUGH" as pcn_name_with_borough,
    "SUB_ICB_CODE" as sub_icb_code,
    "REGISTERED_BOROUGH_NAME" as registered_borough_name,
    "OPEN_DATE" as open_date,
    "CLOSE_DATE" as close_date,
    "PCN_STATUS" as pcn_status,
    "VALID_FROM" as valid_from,
    "VALID_TO" as valid_to,
    "IS_CURRENT" as is_current,
    "SOURCE_SYSTEM" as source_system,
    "CHANGE_REASON" as change_reason,
    "CREATED_BY" as created_by,
    "CREATED_AT" as created_at,
    "HEALTH_BOROUGH_NAME" as health_borough_name
from {{ source('reference_primary_care', 'PCN_HISTORY') }}
