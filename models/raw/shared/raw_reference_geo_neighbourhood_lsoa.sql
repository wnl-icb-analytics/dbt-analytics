{{
    config(
        description="Raw layer (Curated geography lookups - LSOA to neighbourhood, ward, MSOA and ICB hierarchies). 1:1 passthrough with cleaned column names. \nSource: REFERENCE.GEO.NEIGHBOURHOOD_LSOA \ndbt: source(''reference_geo'', ''NEIGHBOURHOOD_LSOA'') \nColumns:\n  LSOA_CODE -> lsoa_code\n  LSOA_NAME -> lsoa_name\n  NEIGHBOURHOOD_CODE -> neighbourhood_code\n  NEIGHBOURHOOD_NAME -> neighbourhood_name\n  REGISTERED_BOROUGH_NAME -> registered_borough_name\n  SUB_ICB_CODE -> sub_icb_code\n  SOURCE_SYSTEM -> source_system"
    )
}}
select
    "LSOA_CODE" as lsoa_code,
    "LSOA_NAME" as lsoa_name,
    "NEIGHBOURHOOD_CODE" as neighbourhood_code,
    "NEIGHBOURHOOD_NAME" as neighbourhood_name,
    "REGISTERED_BOROUGH_NAME" as registered_borough_name,
    "SUB_ICB_CODE" as sub_icb_code,
    "SOURCE_SYSTEM" as source_system
from {{ source('reference_geo', 'NEIGHBOURHOOD_LSOA') }}
