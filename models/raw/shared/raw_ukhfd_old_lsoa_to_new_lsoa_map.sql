{{
    config(
        description="Raw layer: One row per (old LSOA, new LSOA) pair per snapshot; Is_Latest marks the current mapping.. 1:1 passthrough with cleaned column names. \nSource: UKHFD.Old_LSOA_To_New_LSOA_To_LAD.dim_Map_SCD \ndbt: source(''ukhfd_geo'', ''old_lsoa_to_new_lsoa_map'') \nColumns:\n  Old_LSOA_Code -> old_lsoa_code\n  New_LSOA_Code -> new_lsoa_code\n  LAD_Code -> lad_code\n  Is_Latest -> is_latest\n  Effective_From -> effective_from\n  Effective_To -> effective_to"
    )
}}
select
    "Old_LSOA_Code" as old_lsoa_code,
    "New_LSOA_Code" as new_lsoa_code,
    "LAD_Code" as lad_code,
    "Is_Latest" as is_latest,
    "Effective_From" as effective_from,
    "Effective_To" as effective_to
from {{ source('ukhfd_geo', 'old_lsoa_to_new_lsoa_map') }}
