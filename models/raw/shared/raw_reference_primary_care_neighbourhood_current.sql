{{
    config(
        description="Raw layer (Curated primary care organisation reference - practices, PCNs, neighbourhoods and memberships). 1:1 passthrough with cleaned column names. \nSource: REFERENCE.PRIMARY_CARE.NEIGHBOURHOOD_CURRENT \ndbt: source(''reference_primary_care'', ''NEIGHBOURHOOD_CURRENT'') \nColumns:\n  NEIGHBOURHOOD_CODE -> neighbourhood_code\n  NEIGHBOURHOOD_CODE_LEGACY -> neighbourhood_code_legacy\n  NEIGHBOURHOOD_NAME -> neighbourhood_name\n  NEIGHBOURHOOD_NAME_WITH_BOROUGH -> neighbourhood_name_with_borough\n  HEALTH_BOROUGH_NAME -> health_borough_name\n  REGISTERED_BOROUGH_NAME -> registered_borough_name\n  SUB_ICB_CODE -> sub_icb_code\n  ACTIVE_MEMBER_PRACTICE_COUNT -> active_member_practice_count"
    )
}}
select
    "NEIGHBOURHOOD_CODE" as neighbourhood_code,
    "NEIGHBOURHOOD_CODE_LEGACY" as neighbourhood_code_legacy,
    "NEIGHBOURHOOD_NAME" as neighbourhood_name,
    "NEIGHBOURHOOD_NAME_WITH_BOROUGH" as neighbourhood_name_with_borough,
    "HEALTH_BOROUGH_NAME" as health_borough_name,
    "REGISTERED_BOROUGH_NAME" as registered_borough_name,
    "SUB_ICB_CODE" as sub_icb_code,
    "ACTIVE_MEMBER_PRACTICE_COUNT" as active_member_practice_count
from {{ source('reference_primary_care', 'NEIGHBOURHOOD_CURRENT') }}
