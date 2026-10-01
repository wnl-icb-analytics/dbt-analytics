{{
    config(
        description="Raw layer: Emergency care department type codes and descriptions; Is_Latest marks the current row per code.. 1:1 passthrough with cleaned column names. \nSource: UKHFD.Data_Dictionary.dim_Emergency_Care_Department_Type_SCD \ndbt: source(''ukhfd_ecds_data_dictionary'', ''emergency_care_department_type'') \nColumns:\n  Main_Code_Text -> main_code_text\n  Main_Description -> main_description\n  Is_Latest -> is_latest\n  Effective_From -> effective_from"
    )
}}
select
    "Main_Code_Text" as main_code_text,
    "Main_Description" as main_description,
    "Is_Latest" as is_latest,
    "Effective_From" as effective_from
from {{ source('ukhfd_ecds_data_dictionary', 'emergency_care_department_type') }}
