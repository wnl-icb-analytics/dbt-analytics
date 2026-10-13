{{
    config(
        description="Raw layer (Curated geography lookups - LSOA to neighbourhood, ward, MSOA and ICB hierarchies). 1:1 passthrough with cleaned column names. \nSource: REFERENCE.GEO.LSOA_21_WNL \ndbt: source(''reference_geo'', ''LSOA_21_WNL'') \nColumns:\n  AREA_CODE -> area_code\n  AREA_NAME -> area_name\n  MSOA_CODE -> msoa_code\n  MSOA_NAME -> msoa_name\n  WARD_CODE -> ward_code\n  WARD_NAME -> ward_name\n  LAD_CODE -> lad_code\n  LAD_NAME -> lad_name\n  SUB_ICB_CODE -> sub_icb_code\n  IS_WNL -> is_wnl\n  COUNTRY -> country\n  ICB_CODE -> icb_code\n  ICB_NAME -> icb_name\n  NHS_REGION_CODE -> nhs_region_code\n  NHS_REGION_NAME -> nhs_region_name\n  LATITUDE -> latitude\n  LONGITUDE -> longitude\n  GLOBAL_ID -> global_id\n  GEOGRAPHY -> geography"
    )
}}
select
    "AREA_CODE" as area_code,
    "AREA_NAME" as area_name,
    "MSOA_CODE" as msoa_code,
    "MSOA_NAME" as msoa_name,
    "WARD_CODE" as ward_code,
    "WARD_NAME" as ward_name,
    "LAD_CODE" as lad_code,
    "LAD_NAME" as lad_name,
    "SUB_ICB_CODE" as sub_icb_code,
    "IS_WNL" as is_wnl,
    "COUNTRY" as country,
    "ICB_CODE" as icb_code,
    "ICB_NAME" as icb_name,
    "NHS_REGION_CODE" as nhs_region_code,
    "NHS_REGION_NAME" as nhs_region_name,
    "LATITUDE" as latitude,
    "LONGITUDE" as longitude,
    "GLOBAL_ID" as global_id,
    "GEOGRAPHY" as geography
from {{ source('reference_geo', 'LSOA_21_WNL') }}
