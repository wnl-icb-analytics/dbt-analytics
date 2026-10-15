{{
    config(
        description="Raw layer: Reviewed Read v2 to SNOMED CT map rankings, including historical revisions. 1:1 passthrough with cleaned column names. \nSource: UKHFD.Read_Data_Migration.dim_RCT_SCT_Map_Reviewed_SCD \ndbt: source(''ukhfd_read_data_migration'', ''read_v2_to_snomed_map_reviewed'') \nColumns:\n  RC -> rc\n  TC -> tc\n  Rank -> rank\n  Unique_Column -> unique_column\n  In_Source_Data -> in_source_data\n  Import_Date -> import_date\n  Created_Date -> created_date\n  Is_Latest -> is_latest\n  Effective_From -> effective_from\n  Effective_To -> effective_to"
    )
}}
select
    "RC" as rc,
    "TC" as tc,
    "Rank" as rank,
    "Unique_Column" as unique_column,
    "In_Source_Data" as in_source_data,
    "Import_Date" as import_date,
    "Created_Date" as created_date,
    "Is_Latest" as is_latest,
    "Effective_From" as effective_from,
    "Effective_To" as effective_to
from {{ source('ukhfd_read_data_migration', 'read_v2_to_snomed_map_reviewed') }}
