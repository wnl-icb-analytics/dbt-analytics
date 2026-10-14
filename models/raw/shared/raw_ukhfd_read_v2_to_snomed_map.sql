{{
    config(
        description="Raw layer: Read v2 code and term to SNOMED CT maps, including historical revisions. 1:1 passthrough with cleaned column names. \nSource: UKHFD.Read_Data_Migration.dim_RC_SCT_Map2_SCD \ndbt: source(''ukhfd_read_data_migration'', ''read_v2_to_snomed_map'') \nColumns:\n  Map_ID -> map_id\n  Read_Code -> read_code\n  Term_Code -> term_code\n  Concept_ID -> concept_id\n  Description_ID -> description_id\n  Is_Assured -> is_assured\n  Effective_Date -> effective_date\n  Map_Status -> map_status\n  In_Source_Data -> in_source_data\n  Import_Date -> import_date\n  Created_Date -> created_date\n  Is_Latest -> is_latest\n  Effective_From -> effective_from\n  Effective_To -> effective_to"
    )
}}
select
    "Map_ID" as map_id,
    "Read_Code" as read_code,
    "Term_Code" as term_code,
    "Concept_ID" as concept_id,
    "Description_ID" as description_id,
    "Is_Assured" as is_assured,
    "Effective_Date" as effective_date,
    "Map_Status" as map_status,
    "In_Source_Data" as in_source_data,
    "Import_Date" as import_date,
    "Created_Date" as created_date,
    "Is_Latest" as is_latest,
    "Effective_From" as effective_from,
    "Effective_To" as effective_to
from {{ source('ukhfd_read_data_migration', 'read_v2_to_snomed_map') }}
