{{
    config(
        description="Raw layer: Read v2 concept and term to CTV3 maps, including historical revisions. 1:1 passthrough with cleaned column names. \nSource: UKHFD.Read_Data_Migration.dim_RCT_CTV3_Map_SCD \ndbt: source(''ukhfd_read_data_migration'', ''read_v2_to_ctv3_map'') \nColumns:\n  Map_ID -> map_id\n  V2_Concept_ID -> v2_concept_id\n  V2_Term_ID -> v2_term_id\n  CTV3_Term_ID -> ctv3_term_id\n  CTV3_Term_Type -> ctv3_term_type\n  CTV3_Concept_ID -> ctv3_concept_id\n  Use_CTV3_Term_ID -> use_ctv3_term_id\n  Stat -> stat\n  Map_Type -> map_type\n  Map_Status -> map_status\n  Effective_Date -> effective_date\n  Is_assured -> is_assured\n  In_Source_Data -> in_source_data\n  Import_Date -> import_date\n  Created_Date -> created_date\n  Is_Latest -> is_latest\n  Effective_From -> effective_from\n  Effective_To -> effective_to"
    )
}}
select
    "Map_ID" as map_id,
    "V2_Concept_ID" as v2_concept_id,
    "V2_Term_ID" as v2_term_id,
    "CTV3_Term_ID" as ctv3_term_id,
    "CTV3_Term_Type" as ctv3_term_type,
    "CTV3_Concept_ID" as ctv3_concept_id,
    "Use_CTV3_Term_ID" as use_ctv3_term_id,
    "Stat" as stat,
    "Map_Type" as map_type,
    "Map_Status" as map_status,
    "Effective_Date" as effective_date,
    "Is_assured" as is_assured,
    "In_Source_Data" as in_source_data,
    "Import_Date" as import_date,
    "Created_Date" as created_date,
    "Is_Latest" as is_latest,
    "Effective_From" as effective_from,
    "Effective_To" as effective_to
from {{ source('ukhfd_read_data_migration', 'read_v2_to_ctv3_map') }}
