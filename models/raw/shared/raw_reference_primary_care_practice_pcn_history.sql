{{
    config(
        description="Raw layer (Curated primary care organisation reference - practices, PCNs, neighbourhoods and memberships). 1:1 passthrough with cleaned column names. \nSource: REFERENCE.PRIMARY_CARE.PRACTICE_PCN_HISTORY \ndbt: source(''reference_primary_care'', ''PRACTICE_PCN_HISTORY'') \nColumns:\n  PRACTICE_CODE -> practice_code\n  PCN_CODE -> pcn_code\n  PCN_NAME -> pcn_name\n  SUB_ICB_CODE -> sub_icb_code\n  VALID_FROM -> valid_from\n  VALID_TO -> valid_to\n  IS_CURRENT -> is_current\n  SOURCE_SYSTEM -> source_system\n  ODS_RELATIONSHIP_ID -> ods_relationship_id\n  CHANGE_REASON -> change_reason\n  CREATED_BY -> created_by\n  CREATED_AT -> created_at"
    )
}}
select
    "PRACTICE_CODE" as practice_code,
    "PCN_CODE" as pcn_code,
    "PCN_NAME" as pcn_name,
    "SUB_ICB_CODE" as sub_icb_code,
    "VALID_FROM" as valid_from,
    "VALID_TO" as valid_to,
    "IS_CURRENT" as is_current,
    "SOURCE_SYSTEM" as source_system,
    "ODS_RELATIONSHIP_ID" as ods_relationship_id,
    "CHANGE_REASON" as change_reason,
    "CREATED_BY" as created_by,
    "CREATED_AT" as created_at
from {{ source('reference_primary_care', 'PRACTICE_PCN_HISTORY') }}
