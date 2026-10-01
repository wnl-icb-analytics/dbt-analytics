{{
    config(
        description="Raw layer (EMIS extract for c-ltcs pipeline from PID environment via MESH and other local lookups). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.CLTCS.CLTCS_LOCAL_MAPPING_NH_GP \ndbt: source(''cltcs_emis_extract'', ''CLTCS_LOCAL_MAPPING_NH_GP'') \nColumns:\n  PRACTICE_CODE -> practice_code\n  NEIGHBOURHOOD_CODE -> neighbourhood_code\n  PRACTICE_NAME -> practice_name\n  NEIGHBOURHOOD_REGISTERED -> neighbourhood_registered\n  LOCAL_AUTHORITY -> local_authority"
    )
}}
select
    "PRACTICE_CODE" as practice_code,
    "NEIGHBOURHOOD_CODE" as neighbourhood_code,
    "PRACTICE_NAME" as practice_name,
    "NEIGHBOURHOOD_REGISTERED" as neighbourhood_registered,
    "LOCAL_AUTHORITY" as local_authority
from {{ source('cltcs_emis_extract', 'CLTCS_LOCAL_MAPPING_NH_GP') }}
