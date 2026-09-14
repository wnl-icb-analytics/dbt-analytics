{{
    config(
        description="Raw layer (Curated primary care organisation reference - practices, PCNs, neighbourhoods and memberships). 1:1 passthrough with cleaned column names. \nSource: REFERENCE.PRIMARY_CARE.PCN_CURRENT \ndbt: source(''reference_primary_care'', ''PCN_CURRENT'') \nColumns:\n  PCN_CODE -> pcn_code\n  PCN_NAME -> pcn_name\n  PCN_NAME_WITH_BOROUGH -> pcn_name_with_borough\n  SUB_ICB_CODE -> sub_icb_code\n  HEALTH_BOROUGH_NAME -> health_borough_name\n  REGISTERED_BOROUGH_NAME -> registered_borough_name\n  OPEN_DATE -> open_date\n  PCN_STATUS -> pcn_status\n  ACTIVE_MEMBER_PRACTICE_COUNT -> active_member_practice_count"
    )
}}
select
    "PCN_CODE" as pcn_code,
    "PCN_NAME" as pcn_name,
    "PCN_NAME_WITH_BOROUGH" as pcn_name_with_borough,
    "SUB_ICB_CODE" as sub_icb_code,
    "HEALTH_BOROUGH_NAME" as health_borough_name,
    "REGISTERED_BOROUGH_NAME" as registered_borough_name,
    "OPEN_DATE" as open_date,
    "PCN_STATUS" as pcn_status,
    "ACTIVE_MEMBER_PRACTICE_COUNT" as active_member_practice_count
from {{ source('reference_primary_care', 'PCN_CURRENT') }}
