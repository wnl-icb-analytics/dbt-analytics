{{
    config(
        description="Raw layer: One row per LSOA on December 2021 boundaries with its ONS code and name.. 1:1 passthrough with cleaned column names. \nSource: UKHFD.Ordnance_Survey.dim_LSOA_DEC_2021_EW_BFE \ndbt: source(''ukhfd_ordnance_survey'', ''lsoa_2021'') \nColumns:\n  ID -> id\n  LSOA21CD -> lsoa21_cd\n  LSOA21NM -> lsoa21_nm\n  GlobalID -> global_id"
    )
}}
select
    "ID" as id,
    "LSOA21CD" as lsoa21_cd,
    "LSOA21NM" as lsoa21_nm,
    "GlobalID" as global_id
from {{ source('ukhfd_ordnance_survey', 'lsoa_2021') }}
