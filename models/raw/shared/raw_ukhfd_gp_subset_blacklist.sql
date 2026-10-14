{{
    config(
        description="Raw layer: GP subset blacklist entries and reasons, including historical revisions. 1:1 passthrough with cleaned column names. \nSource: UKHFD.Read_Data_Migration.dim_GP_Subset_Blacklist_SCD \ndbt: source(''ukhfd_read_data_migration'', ''gp_subset_blacklist'') \nColumns:\n  SCUI -> scui\n  STUI -> stui\n  Map -> map\n  Term -> term\n  Reason -> reason\n  Unique_Column -> unique_column\n  In_Source_Data -> in_source_data\n  Import_Date -> import_date\n  Created_Date -> created_date\n  Is_Latest -> is_latest\n  Effective_From -> effective_from\n  Effective_To -> effective_to"
    )
}}
select
    "SCUI" as scui,
    "STUI" as stui,
    "Map" as map,
    "Term" as term,
    "Reason" as reason,
    "Unique_Column" as unique_column,
    "In_Source_Data" as in_source_data,
    "Import_Date" as import_date,
    "Created_Date" as created_date,
    "Is_Latest" as is_latest,
    "Effective_From" as effective_from,
    "Effective_To" as effective_to
from {{ source('ukhfd_read_data_migration', 'gp_subset_blacklist') }}
