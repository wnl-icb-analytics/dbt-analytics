{{
    config(
        description="Raw layer: One row per LSOA on 2011 boundaries with its ONS code and name.. 1:1 passthrough with cleaned column names. \nSource: UKHFD.Ordnance_Survey.dim_LSOA_2011 \ndbt: source(''ukhfd_ordnance_survey'', ''lsoa_2011'') \nColumns:\n  ID -> id\n  LSOA11CD -> lsoa11_cd\n  LSOA11NM -> lsoa11_nm"
    )
}}
select
    "ID" as id,
    "LSOA11CD" as lsoa11_cd,
    "LSOA11NM" as lsoa11_nm
from {{ source('ukhfd_ordnance_survey', 'lsoa_2011') }}
