{{
    config(
        description="Raw layer: CTV3 concept to CRE maps, including historical revisions.. 1:1 passthrough with cleaned column names. \nSource: UKHFD.Read_Data_Migration.dim_CTV3_CRE_Map_SCD \ndbt: source(''ukhfd_read_data_migration'', ''ctv3_to_cre_map'') \nColumns:\n  Map_ID -> map_id\n  CTV3_Concept_ID -> ctv3_concept_id\n  CRE_ID -> cre_id\n  Map_Status -> map_status\n  Effective_Date -> effective_date\n  In_Source_Data -> in_source_data\n  Import_Date -> import_date\n  Created_Date -> created_date\n  Is_Latest -> is_latest\n  Effective_From -> effective_from\n  Effective_To -> effective_to"
    )
}}
select
    "Map_ID" as map_id,
    "CTV3_Concept_ID" as ctv3_concept_id,
    "CRE_ID" as cre_id,
    "Map_Status" as map_status,
    "Effective_Date" as effective_date,
    "In_Source_Data" as in_source_data,
    "Import_Date" as import_date,
    "Created_Date" as created_date,
    "Is_Latest" as is_latest,
    "Effective_From" as effective_from,
    "Effective_To" as effective_to
from {{ source('ukhfd_read_data_migration', 'ctv3_to_cre_map') }}
