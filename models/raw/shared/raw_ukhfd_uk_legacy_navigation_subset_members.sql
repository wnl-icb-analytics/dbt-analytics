{{
    config(
        description="Raw layer: UK legacy navigation subset members, including historical revisions.. 1:1 passthrough with cleaned column names. \nSource: UKHFD.Read_Data_Migration.dim_Subset_Members_UK_Legacy_Navigation_SCD \ndbt: source(''ukhfd_read_data_migration'', ''uk_legacy_navigation_subset_members'') \nColumns:\n  Subset_ID -> subset_id\n  Member_ID -> member_id\n  Member_Status -> member_status\n  Linked_ID -> linked_id\n  Unique_Column -> unique_column\n  In_Source_Data -> in_source_data\n  Import_Date -> import_date\n  Created_Date -> created_date\n  Is_Latest -> is_latest\n  Effective_From -> effective_from\n  Effective_To -> effective_to"
    )
}}
select
    "Subset_ID" as subset_id,
    "Member_ID" as member_id,
    "Member_Status" as member_status,
    "Linked_ID" as linked_id,
    "Unique_Column" as unique_column,
    "In_Source_Data" as in_source_data,
    "Import_Date" as import_date,
    "Created_Date" as created_date,
    "Is_Latest" as is_latest,
    "Effective_From" as effective_from,
    "Effective_To" as effective_to
from {{ source('ukhfd_read_data_migration', 'uk_legacy_navigation_subset_members') }}
