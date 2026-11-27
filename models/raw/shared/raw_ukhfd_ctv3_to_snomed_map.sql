{{
    config(
        description="Raw layer: CTV3 concept and term to SNOMED CT maps, including historical revisions.. 1:1 passthrough with cleaned column names. \nSource: UKHFD.Read_Data_Migration.dim_CTV3_SCT_Map2_SCD \ndbt: source(''ukhfd_read_data_migration'', ''ctv3_to_snomed_map'') \nColumns:\n  Map_ID -> map_id\n  CTV3_Concept_ID -> ctv3_concept_id\n  CTV3_TermID -> ctv3_term_id\n  CTV3_Term_Type -> ctv3_term_type\n  SCT_Concept_ID -> sct_concept_id\n  SCT_Description_ID -> sct_description_id\n  Map_Status -> map_status\n  Effective_Date -> effective_date\n  Is_Assured -> is_assured\n  In_Source_Data -> in_source_data\n  Import_Date -> import_date\n  Created_Date -> created_date\n  Is_Latest -> is_latest\n  Effective_From -> effective_from\n  Effective_To -> effective_to"
    )
}}
select
    "Map_ID" as map_id,
    "CTV3_Concept_ID" as ctv3_concept_id,
    "CTV3_TermID" as ctv3_term_id,
    "CTV3_Term_Type" as ctv3_term_type,
    "SCT_Concept_ID" as sct_concept_id,
    "SCT_Description_ID" as sct_description_id,
    "Map_Status" as map_status,
    "Effective_Date" as effective_date,
    "Is_Assured" as is_assured,
    "In_Source_Data" as in_source_data,
    "Import_Date" as import_date,
    "Created_Date" as created_date,
    "Is_Latest" as is_latest,
    "Effective_From" as effective_from,
    "Effective_To" as effective_to
from {{ source('ukhfd_read_data_migration', 'ctv3_to_snomed_map') }}
