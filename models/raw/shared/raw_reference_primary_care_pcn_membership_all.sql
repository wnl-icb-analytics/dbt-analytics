{{
    config(
        description="Raw layer (Curated primary care organisation reference - practices, PCNs, neighbourhoods and memberships). 1:1 passthrough with cleaned column names. \nSource: REFERENCE.PRIMARY_CARE.PCN_MEMBERSHIP_ALL \ndbt: source(''reference_primary_care'', ''PCN_MEMBERSHIP_ALL'') \nColumns:\n  PCN_CODE -> pcn_code\n  PCN_NAME -> pcn_name\n  PCN_NAME_WITH_BOROUGH -> pcn_name_with_borough\n  PRACTICE_CODE -> practice_code\n  PRACTICE_NAME -> practice_name\n  PRACTICE_STATUS -> practice_status\n  SUB_ICB_CODE -> sub_icb_code\n  HEALTH_BOROUGH_NAME -> health_borough_name\n  REGISTERED_BOROUGH_NAME -> registered_borough_name"
    )
}}
select
    "PCN_CODE" as pcn_code,
    "PCN_NAME" as pcn_name,
    "PCN_NAME_WITH_BOROUGH" as pcn_name_with_borough,
    "PRACTICE_CODE" as practice_code,
    "PRACTICE_NAME" as practice_name,
    "PRACTICE_STATUS" as practice_status,
    "SUB_ICB_CODE" as sub_icb_code,
    "HEALTH_BOROUGH_NAME" as health_borough_name,
    "REGISTERED_BOROUGH_NAME" as registered_borough_name
from {{ source('reference_primary_care', 'PCN_MEMBERSHIP_ALL') }}
