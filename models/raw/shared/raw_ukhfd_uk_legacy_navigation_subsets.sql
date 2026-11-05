{{
    config(
        description="Raw layer: UK legacy navigation subset definitions, including historical revisions.. 1:1 passthrough with cleaned column names. \nSource: UKHFD.Read_Data_Migration.dim_Subsets_UK_Legacy_Navigation_SCD \ndbt: source(''ukhfd_read_data_migration'', ''uk_legacy_navigation_subsets'') \nColumns:\n  Subset_ID -> subset_id\n  Subset_Original_ID -> subset_original_id\n  Subset_Version -> subset_version\n  Subset_Name -> subset_name\n  Subset_Type -> subset_type\n  Language_Code -> language_code\n  Subset_Realm_ID -> subset_realm_id\n  Context_ID -> context_id\n  In_Source_Data -> in_source_data\n  Import_Date -> import_date\n  Created_Date -> created_date\n  Is_Latest -> is_latest\n  Effective_From -> effective_from\n  Effective_To -> effective_to"
    )
}}
select
    "Subset_ID" as subset_id,
    "Subset_Original_ID" as subset_original_id,
    "Subset_Version" as subset_version,
    "Subset_Name" as subset_name,
    "Subset_Type" as subset_type,
    "Language_Code" as language_code,
    "Subset_Realm_ID" as subset_realm_id,
    "Context_ID" as context_id,
    "In_Source_Data" as in_source_data,
    "Import_Date" as import_date,
    "Created_Date" as created_date,
    "Is_Latest" as is_latest,
    "Effective_From" as effective_from,
    "Effective_To" as effective_to
from {{ source('ukhfd_read_data_migration', 'uk_legacy_navigation_subsets') }}
