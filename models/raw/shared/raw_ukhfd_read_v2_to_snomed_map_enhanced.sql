{{
    config(
        description="Raw layer: Enhanced Read v2 to SNOMED CT maps with term identifiers, including historical revisions. 1:1 passthrough with cleaned column names. \nSource: UKHFD.Read_Data_Migration.dim_RC_SCT_Map_Enhanced_SCD \ndbt: source(''ukhfd_read_data_migration'', ''read_v2_to_snomed_map_enhanced'') \nColumns:\n  Map_ID -> map_id\n  Read_Code -> read_code\n  Term_Code -> term_code\n  Concept_ID -> concept_id\n  T30_ID -> t30_id\n  T60_ID -> t60_id\n  T198_ID -> t198_id\n  Effective_Date -> effective_date\n  Map_Status -> map_status\n  In_Source_Data -> in_source_data\n  Import_Date -> import_date\n  Created_Date -> created_date\n  Is_Latest -> is_latest\n  Effective_From -> effective_from\n  Effective_To -> effective_to"
    )
}}
select
    "Map_ID" as map_id,
    "Read_Code" as read_code,
    "Term_Code" as term_code,
    "Concept_ID" as concept_id,
    "T30_ID" as t30_id,
    "T60_ID" as t60_id,
    "T198_ID" as t198_id,
    "Effective_Date" as effective_date,
    "Map_Status" as map_status,
    "In_Source_Data" as in_source_data,
    "Import_Date" as import_date,
    "Created_Date" as created_date,
    "Is_Latest" as is_latest,
    "Effective_From" as effective_from,
    "Effective_To" as effective_to
from {{ source('ukhfd_read_data_migration', 'read_v2_to_snomed_map_enhanced') }}
