{{
    config(
        description="Raw layer (Data management reference datasets). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.DATA_MANAGEMENT.NEIGHBOURHOODS_LSOAS_LOOKUP_20260505 \ndbt: source(''reference_data_management'', ''NEIGHBOURHOODS_LSOAS_LOOKUP_20260505'') \nColumns:\n  LSOA -> lsoa\n  WARD -> ward\n  NEIGHBOURHOOD -> neighbourhood\n  BOROUGH -> borough\n  ICB -> icb"
    )
}}
select
    "LSOA" as lsoa,
    "WARD" as ward,
    "NEIGHBOURHOOD" as neighbourhood,
    "BOROUGH" as borough,
    "ICB" as icb
from {{ source('reference_data_management', 'NEIGHBOURHOODS_LSOAS_LOOKUP_20260505') }}
