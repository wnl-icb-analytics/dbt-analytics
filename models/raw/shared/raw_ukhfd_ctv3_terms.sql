{{
    config(
        description="Raw layer: CTV3 term identifiers and text, including historical revisions. 1:1 passthrough with cleaned column names. \nSource: UKHFD.Read_Codes.dim_CV3_Terms_SCD \ndbt: source(''ukhfd_read_codes'', ''ctv3_terms'') \nColumns:\n  Term_ID -> term_id\n  Term_Status -> term_status\n  Term_30 -> term_30\n  Term_60 -> term_60\n  Term_198 -> term_198\n  In_Source_Data -> in_source_data\n  Import_Date -> import_date\n  Created_Date -> created_date\n  Is_Latest -> is_latest\n  Effective_From -> effective_from\n  Effective_To -> effective_to"
    )
}}
select
    "Term_ID" as term_id,
    "Term_Status" as term_status,
    "Term_30" as term_30,
    "Term_60" as term_60,
    "Term_198" as term_198,
    "In_Source_Data" as in_source_data,
    "Import_Date" as import_date,
    "Created_Date" as created_date,
    "Is_Latest" as is_latest,
    "Effective_From" as effective_from,
    "Effective_To" as effective_to
from {{ source('ukhfd_read_codes', 'ctv3_terms') }}
