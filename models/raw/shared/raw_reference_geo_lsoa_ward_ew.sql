{{
    config(
        description="Raw layer (Curated geography lookups - LSOA to neighbourhood, ward, MSOA and ICB hierarchies). 1:1 passthrough with cleaned column names. \nSource: REFERENCE.GEO.LSOA_WARD_EW \ndbt: source(''reference_geo'', ''LSOA_WARD_EW'') \nColumns:\n  LSOA_CODE -> lsoa_code\n  LSOA_NAME -> lsoa_name\n  WARD_CODE -> ward_code\n  WARD_NAME -> ward_name\n  LAD_CODE -> lad_code\n  LAD_NAME -> lad_name"
    )
}}
select
    "LSOA_CODE" as lsoa_code,
    "LSOA_NAME" as lsoa_name,
    "WARD_CODE" as ward_code,
    "WARD_NAME" as ward_name,
    "LAD_CODE" as lad_code,
    "LAD_NAME" as lad_name
from {{ source('reference_geo', 'LSOA_WARD_EW') }}
