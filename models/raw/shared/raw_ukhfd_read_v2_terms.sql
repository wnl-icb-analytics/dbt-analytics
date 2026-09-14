{{
    config(
        description="Raw layer: Read v2 term definitions, including historical revisions. 1:1 passthrough with cleaned column names. \nSource: UKHFD.Read_Codes.dim_KV2_SCD \ndbt: source(''ukhfd_read_codes'', ''read_v2_terms'') \nColumns:\n  Term_Key -> term_key\n  Uniquifier -> uniquifier\n  Term_30 -> term_30\n  Term_60 -> term_60\n  Term_198 -> term_198\n  Term_Code -> term_code\n  Language_Code -> language_code\n  Read_Code -> read_code\n  Status_Flag -> status_flag\n  Source_File_Name -> source_file_name\n  Unique_Column -> unique_column\n  In_Source_Data -> in_source_data\n  Import_Date -> import_date\n  Created_Date -> created_date\n  Is_Latest -> is_latest\n  Effective_From -> effective_from\n  Effective_To -> effective_to"
    )
}}
select
    "Term_Key" as term_key,
    "Uniquifier" as uniquifier,
    "Term_30" as term_30,
    "Term_60" as term_60,
    "Term_198" as term_198,
    "Term_Code" as term_code,
    "Language_Code" as language_code,
    "Read_Code" as read_code,
    "Status_Flag" as status_flag,
    "Source_File_Name" as source_file_name,
    "Unique_Column" as unique_column,
    "In_Source_Data" as in_source_data,
    "Import_Date" as import_date,
    "Created_Date" as created_date,
    "Is_Latest" as is_latest,
    "Effective_From" as effective_from,
    "Effective_To" as effective_to
from {{ source('ukhfd_read_codes', 'read_v2_terms') }}
