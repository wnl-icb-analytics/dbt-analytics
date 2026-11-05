{{
    config(
        description="Raw layer (Curated geography lookups - LSOA to neighbourhood, ward, MSOA and ICB hierarchies). 1:1 passthrough with cleaned column names. \nSource: REFERENCE.GEO.NEIGHBOURHOOD_WARD \ndbt: source(''reference_geo'', ''NEIGHBOURHOOD_WARD'') \nColumns:\n  WARD_CODE -> ward_code\n  WARD_NAME -> ward_name\n  LAD_NAME -> lad_name\n  NEIGHBOURHOOD_CODE -> neighbourhood_code\n  NEIGHBOURHOOD_NAME -> neighbourhood_name\n  REGISTERED_BOROUGH_NAME -> registered_borough_name\n  SUB_ICB_CODE -> sub_icb_code\n  SOURCE -> source\n  UPDATED_BY -> updated_by\n  UPDATED_AT -> updated_at"
    )
}}
select
    "WARD_CODE" as ward_code,
    "WARD_NAME" as ward_name,
    "LAD_NAME" as lad_name,
    "NEIGHBOURHOOD_CODE" as neighbourhood_code,
    "NEIGHBOURHOOD_NAME" as neighbourhood_name,
    "REGISTERED_BOROUGH_NAME" as registered_borough_name,
    "SUB_ICB_CODE" as sub_icb_code,
    "SOURCE" as source,
    "UPDATED_BY" as updated_by,
    "UPDATED_AT" as updated_at
from {{ source('reference_geo', 'NEIGHBOURHOOD_WARD') }}
