{{
    config(
        description="Raw layer: ISO 639 language codes and their UKHFD history.. 1:1 passthrough with cleaned column names. \nSource: UKHFD.Other.dim_ISO_Language_Codes_SCD \ndbt: source(''ukhfd_other'', ''iso_language_codes'') \nColumns:\n  ISO_639_2_Code -> iso_639_2_code\n  ISO_639_1_Code -> iso_639_1_code\n  English_Name_Of_Language -> english_name_of_language\n  French_Name_Of_Language -> french_name_of_language\n  German_Name_Of_Language -> german_name_of_language\n  In_Source_Data -> in_source_data\n  Import_Date -> import_date\n  Created_Date -> created_date\n  Is_Latest -> is_latest\n  Effective_From -> effective_from\n  Effective_To -> effective_to"
    )
}}
select
    "ISO_639_2_Code" as iso_639_2_code,
    "ISO_639_1_Code" as iso_639_1_code,
    "English_Name_Of_Language" as english_name_of_language,
    "French_Name_Of_Language" as french_name_of_language,
    "German_Name_Of_Language" as german_name_of_language,
    "In_Source_Data" as in_source_data,
    "Import_Date" as import_date,
    "Created_Date" as created_date,
    "Is_Latest" as is_latest,
    "Effective_From" as effective_from,
    "Effective_To" as effective_to
from {{ source('ukhfd_other', 'iso_language_codes') }}
