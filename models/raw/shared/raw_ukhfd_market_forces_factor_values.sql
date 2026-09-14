{{
    config(
        description="Raw layer: Provider Market Forces Factor values from NHS tariff and payment workbooks.. 1:1 passthrough with cleaned column names. \nSource: UKHFD.PbR_v2.dim_MFF_Values \ndbt: source(''ukhfd_pbr'', ''market_forces_factor_values'') \nColumns:\n  Organisation_Code -> organisation_code\n  Organisation_Name -> organisation_name\n  MFF_Value -> mff_value\n  MFF_Value_Year_2 -> mff_value_year_2\n  MFF_Value_Year_3 -> mff_value_year_3\n  MFF_Value_Year_4 -> mff_value_year_4\n  MFF_Value_Year_5 -> mff_value_year_5\n  Notes -> notes\n  File_Name -> file_name\n  Import_Date -> import_date\n  Created_Date -> created_date"
    )
}}
select
    "Organisation_Code" as organisation_code,
    "Organisation_Name" as organisation_name,
    "MFF_Value" as mff_value,
    "MFF_Value_Year_2" as mff_value_year_2,
    "MFF_Value_Year_3" as mff_value_year_3,
    "MFF_Value_Year_4" as mff_value_year_4,
    "MFF_Value_Year_5" as mff_value_year_5,
    "Notes" as notes,
    "File_Name" as file_name,
    "Import_Date" as import_date,
    "Created_Date" as created_date
from {{ source('ukhfd_pbr', 'market_forces_factor_values') }}
