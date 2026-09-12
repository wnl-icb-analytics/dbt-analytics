{{
    config(
        description="Raw layer: CTV3 concept identifiers and status, including historical revisions. 1:1 passthrough with cleaned column names. \nSource: UKHFD.Read_Codes.dim_CV3_Concept_SCD \ndbt: source(''ukhfd_read_codes'', ''ctv3_concepts'') \nColumns:\n  Read_Code -> read_code\n  Concept_Status -> concept_status\n  Linguistic_Role -> linguistic_role\n  Subject_Type -> subject_type\n  In_Source_Data -> in_source_data\n  Import_Date -> import_date\n  Created_Date -> created_date\n  Is_Latest -> is_latest\n  Effective_From -> effective_from\n  Effective_To -> effective_to"
    )
}}
select
    "Read_Code" as read_code,
    "Concept_Status" as concept_status,
    "Linguistic_Role" as linguistic_role,
    "Subject_Type" as subject_type,
    "In_Source_Data" as in_source_data,
    "Import_Date" as import_date,
    "Created_Date" as created_date,
    "Is_Latest" as is_latest,
    "Effective_From" as effective_from,
    "Effective_To" as effective_to
from {{ source('ukhfd_read_codes', 'ctv3_concepts') }}
