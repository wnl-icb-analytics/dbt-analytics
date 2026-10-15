{{
    config(
        description="Raw layer: Read v2 term text to SNOMED CT concept maps, including historical revisions. 1:1 passthrough with cleaned column names. \nSource: UKHFD.Read_Data_Migration.dim_RC_Term_SCT_Map_SCD \ndbt: source(''ukhfd_read_data_migration'', ''read_v2_term_to_snomed_map'') \nColumns:\n  Read_Code -> read_code\n  Term -> term\n  Concept_ID -> concept_id\n  Map_ID -> map_id\n  In_Source_Data -> in_source_data\n  Import_Date -> import_date\n  Created_Date -> created_date\n  Is_Latest -> is_latest\n  Effective_From -> effective_from\n  Effective_To -> effective_to"
    )
}}
select
    "Read_Code" as read_code,
    "Term" as term,
    "Concept_ID" as concept_id,
    "Map_ID" as map_id,
    "In_Source_Data" as in_source_data,
    "Import_Date" as import_date,
    "Created_Date" as created_date,
    "Is_Latest" as is_latest,
    "Effective_From" as effective_from,
    "Effective_To" as effective_to
from {{ source('ukhfd_read_data_migration', 'read_v2_term_to_snomed_map') }}
