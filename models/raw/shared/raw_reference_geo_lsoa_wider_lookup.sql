{{
    config(
        description="Raw layer (Curated geography lookups - LSOA to neighbourhood, ward, MSOA and ICB hierarchies). 1:1 passthrough with cleaned column names. \nSource: REFERENCE.GEO.LSOA_WIDER_LOOKUP \ndbt: source(''reference_geo'', ''LSOA_WIDER_LOOKUP'') \nColumns:\n  LSOA_CODE -> lsoa_code\n  MSOA_CODE -> msoa_code\n  MSOA_NAME -> msoa_name\n  LAD_CODE -> lad_code"
    )
}}
select
    "LSOA_CODE" as lsoa_code,
    "MSOA_CODE" as msoa_code,
    "MSOA_NAME" as msoa_name,
    "LAD_CODE" as lad_code
from {{ source('reference_geo', 'LSOA_WIDER_LOOKUP') }}
