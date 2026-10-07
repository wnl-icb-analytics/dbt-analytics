{{
    config(
        description="Raw layer (Curated primary care organisation reference - practices, PCNs, neighbourhoods and memberships). 1:1 passthrough with cleaned column names. \nSource: REFERENCE.PRIMARY_CARE.NEIGHBOURHOOD \ndbt: source(''reference_primary_care'', ''NEIGHBOURHOOD'') \nColumns:\n  NEIGHBOURHOOD_CODE -> neighbourhood_code\n  NEIGHBOURHOOD_NAME -> neighbourhood_name\n  NEIGHBOURHOOD_NAME_WITH_BOROUGH -> neighbourhood_name_with_borough\n  REGISTERED_BOROUGH_NAME -> registered_borough_name\n  SUB_ICB_CODE -> sub_icb_code\n  IS_ACTIVE -> is_active\n  UPDATED_BY -> updated_by\n  UPDATED_AT -> updated_at\n  NEIGHBOURHOOD_CODE_LEGACY -> neighbourhood_code_legacy\n  HEALTH_BOROUGH_NAME -> health_borough_name"
    )
}}
select
    "NEIGHBOURHOOD_CODE" as neighbourhood_code,
    "NEIGHBOURHOOD_NAME" as neighbourhood_name,
    "NEIGHBOURHOOD_NAME_WITH_BOROUGH" as neighbourhood_name_with_borough,
    "REGISTERED_BOROUGH_NAME" as registered_borough_name,
    "SUB_ICB_CODE" as sub_icb_code,
    "IS_ACTIVE" as is_active,
    "UPDATED_BY" as updated_by,
    "UPDATED_AT" as updated_at,
    "NEIGHBOURHOOD_CODE_LEGACY" as neighbourhood_code_legacy,
    "HEALTH_BOROUGH_NAME" as health_borough_name
from {{ source('reference_primary_care', 'NEIGHBOURHOOD') }}
