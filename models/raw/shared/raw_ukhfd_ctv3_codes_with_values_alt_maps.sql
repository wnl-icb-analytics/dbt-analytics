{{
    config(
        description="Raw layer: Alternative observable SNOMED CT maps for CTV3 codes with values, including historical revisions.. 1:1 passthrough with cleaned column names. \nSource: UKHFD.Read_Data_Migration.dim_Codes_With_Values_Alt_Maps_CTV3_SCD \ndbt: source(''ukhfd_read_data_migration'', ''ctv3_codes_with_values_alt_maps'') \nColumns:\n  CTV3ID -> ctv3_id\n  TermCode -> term_code\n  Observable_ConceptID -> observable_concept_id\n  Observable_DescriptionID -> observable_description_id\n  Use_Alternate -> use_alternate\n  In_Source_Data -> in_source_data\n  Import_Date -> import_date\n  Created_Date -> created_date\n  Is_Latest -> is_latest\n  Effective_From -> effective_from\n  Effective_To -> effective_to"
    )
}}
select
    "CTV3ID" as ctv3_id,
    "TermCode" as term_code,
    "Observable_ConceptID" as observable_concept_id,
    "Observable_DescriptionID" as observable_description_id,
    "Use_Alternate" as use_alternate,
    "In_Source_Data" as in_source_data,
    "Import_Date" as import_date,
    "Created_Date" as created_date,
    "Is_Latest" as is_latest,
    "Effective_From" as effective_from,
    "Effective_To" as effective_to
from {{ source('ukhfd_read_data_migration', 'ctv3_codes_with_values_alt_maps') }}
