{{
    config(
        description="Raw layer: Primary care reference set members and linked components, including historical revisions.. 1:1 passthrough with cleaned column names. \nSource: UKHFD.Read_Data_Migration.dim_Ref_Set_Primary_Care_SCD \ndbt: source(''ukhfd_read_data_migration'', ''primary_care_refset'') \nColumns:\n  ID -> id\n  Effective_Time -> effective_time\n  Active -> active\n  Module_ID -> module_id\n  Ref_Set_ID -> ref_set_id\n  Referenced_Component_ID -> referenced_component_id\n  Linked_Component_ID -> linked_component_id\n  Linked_String_1 -> linked_string_1\n  Linked_String_2 -> linked_string_2\n  In_Source_Data -> in_source_data\n  Import_Date -> import_date\n  Created_Date -> created_date\n  Is_Latest -> is_latest\n  Effective_From -> effective_from\n  Effective_To -> effective_to"
    )
}}
select
    "ID" as id,
    "Effective_Time" as effective_time,
    "Active" as active,
    "Module_ID" as module_id,
    "Ref_Set_ID" as ref_set_id,
    "Referenced_Component_ID" as referenced_component_id,
    "Linked_Component_ID" as linked_component_id,
    "Linked_String_1" as linked_string_1,
    "Linked_String_2" as linked_string_2,
    "In_Source_Data" as in_source_data,
    "Import_Date" as import_date,
    "Created_Date" as created_date,
    "Is_Latest" as is_latest,
    "Effective_From" as effective_from,
    "Effective_To" as effective_to
from {{ source('ukhfd_read_data_migration', 'primary_care_refset') }}
