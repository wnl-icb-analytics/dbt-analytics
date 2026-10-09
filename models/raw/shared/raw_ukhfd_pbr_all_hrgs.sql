{{
    config(
        description="Raw layer: HRG codes and names across HRG4+ releases; Is_Latest marks the newest row per code.. 1:1 passthrough with cleaned column names. \nSource: UKHFD.PbR.dim_All_HRGs_SCD \ndbt: source(''ukhfd_pbr_hrg'', ''all_hrgs'') \nColumns:\n  HRG_Code -> hrg_code\n  HRG_Name -> hrg_name\n  Source_Table -> source_table\n  In_Source_Data -> in_source_data\n  Import_Date -> import_date\n  Created_Date -> created_date\n  Is_Latest -> is_latest\n  Effective_From -> effective_from\n  Effective_To -> effective_to"
    )
}}
select
    "HRG_Code" as hrg_code,
    "HRG_Name" as hrg_name,
    "Source_Table" as source_table,
    "In_Source_Data" as in_source_data,
    "Import_Date" as import_date,
    "Created_Date" as created_date,
    "Is_Latest" as is_latest,
    "Effective_From" as effective_from,
    "Effective_To" as effective_to
from {{ source('ukhfd_pbr_hrg', 'all_hrgs') }}
