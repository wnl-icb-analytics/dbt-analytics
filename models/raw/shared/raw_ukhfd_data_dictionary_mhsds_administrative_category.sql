{{
    config(
        description="Raw layer: Administrative category definitions and their UKHFD history.. 1:1 passthrough with cleaned column names. \nSource: UKHFD.Data_Dictionary.dim_Administrative_Category_Code_SCD \ndbt: source(''ukhfd_data_dictionary'', ''mhsds_administrative_category'') \nColumns:\n  Attr_Name -> attr_name\n  Valid_From -> valid_from\n  Valid_To -> valid_to\n  Main_Code_Text -> main_code_text\n  Sub_Code1_Text -> sub_code1_text\n  Sub_Code2_Text -> sub_code2_text\n  Sub_Code3_Text -> sub_code3_text\n  Major_Category -> major_category\n  Category -> category\n  Main_Description -> main_description\n  Main_Description_60_Chars -> main_description_60_chars\n  Sub1_Description -> sub1_description\n  Sub2_Description -> sub2_description\n  Sub3_Description -> sub3_description\n  Notes -> notes\n  In_Source_Table -> in_source_table\n  Unique_Column -> unique_column\n  Import_Date -> import_date\n  Created_Date -> created_date\n  Is_Latest -> is_latest\n  Effective_From -> effective_from\n  Effective_To -> effective_to"
    )
}}
select
    "Attr_Name" as attr_name,
    "Valid_From" as valid_from,
    "Valid_To" as valid_to,
    "Main_Code_Text" as main_code_text,
    "Sub_Code1_Text" as sub_code1_text,
    "Sub_Code2_Text" as sub_code2_text,
    "Sub_Code3_Text" as sub_code3_text,
    "Major_Category" as major_category,
    "Category" as category,
    "Main_Description" as main_description,
    "Main_Description_60_Chars" as main_description_60_chars,
    "Sub1_Description" as sub1_description,
    "Sub2_Description" as sub2_description,
    "Sub3_Description" as sub3_description,
    "Notes" as notes,
    "In_Source_Table" as in_source_table,
    "Unique_Column" as unique_column,
    "Import_Date" as import_date,
    "Created_Date" as created_date,
    "Is_Latest" as is_latest,
    "Effective_From" as effective_from,
    "Effective_To" as effective_to
from {{ source('ukhfd_data_dictionary', 'mhsds_administrative_category') }}
