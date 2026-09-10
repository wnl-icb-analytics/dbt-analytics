{{
    config(
        description="Raw layer: ODS closed organisation and site archive since 1991, including historical revisions.. 1:1 passthrough with cleaned column names. \nSource: UKHFD.ODS.dim_Orgs_Closed_Since_1991_SCD \ndbt: source(''ukhfd_ods'', ''closed_organisations'') \nColumns:\n  Organisation_Code -> organisation_code\n  Organisation_Name -> organisation_name\n  National_Grouping_Code -> national_grouping_code\n  High_Level_Health_Authority_Code -> high_level_health_authority_code\n  Address_Line_1 -> address_line_1\n  Address_Line_2 -> address_line_2\n  Address_Line_3 -> address_line_3\n  Address_Line_4 -> address_line_4\n  Address_Line_5 -> address_line_5\n  Postcode -> postcode\n  Open_Date -> open_date\n  Close_Date -> close_date\n  Unused_Field_13 -> unused_field_13\n  Organisation_Sub_Type_Code -> organisation_sub_type_code\n  Parent_Organisation_Code -> parent_organisation_code\n  Join_Parent_Date -> join_parent_date\n  Left_Parent_Date -> left_parent_date\n  Contact_Telephone_Number -> contact_telephone_number\n  Unused_Field_19 -> unused_field_19\n  Unused_Field_20 -> unused_field_20\n  Unused_Field_21 -> unused_field_21\n  Amended_Record_Indicator -> amended_record_indicator\n  Wave_Number -> wave_number\n  Unused_Field_24 -> unused_field_24\n  Unused_Field_25 -> unused_field_25\n  Unused_Field_26 -> unused_field_26\n  Unused_Field_27 -> unused_field_27\n  In_Source_Data -> in_source_data\n  Import_Date -> import_date\n  Created_Date -> created_date\n  Is_Latest -> is_latest\n  Effective_From -> effective_from\n  Effective_To -> effective_to\n  Char_8_ASCII_Index -> char_8_ascii_index"
    )
}}
select
    "Organisation_Code" as organisation_code,
    "Organisation_Name" as organisation_name,
    "National_Grouping_Code" as national_grouping_code,
    "High_Level_Health_Authority_Code" as high_level_health_authority_code,
    "Address_Line_1" as address_line_1,
    "Address_Line_2" as address_line_2,
    "Address_Line_3" as address_line_3,
    "Address_Line_4" as address_line_4,
    "Address_Line_5" as address_line_5,
    "Postcode" as postcode,
    "Open_Date" as open_date,
    "Close_Date" as close_date,
    "Unused_Field_13" as unused_field_13,
    "Organisation_Sub_Type_Code" as organisation_sub_type_code,
    "Parent_Organisation_Code" as parent_organisation_code,
    "Join_Parent_Date" as join_parent_date,
    "Left_Parent_Date" as left_parent_date,
    "Contact_Telephone_Number" as contact_telephone_number,
    "Unused_Field_19" as unused_field_19,
    "Unused_Field_20" as unused_field_20,
    "Unused_Field_21" as unused_field_21,
    "Amended_Record_Indicator" as amended_record_indicator,
    "Wave_Number" as wave_number,
    "Unused_Field_24" as unused_field_24,
    "Unused_Field_25" as unused_field_25,
    "Unused_Field_26" as unused_field_26,
    "Unused_Field_27" as unused_field_27,
    "In_Source_Data" as in_source_data,
    "Import_Date" as import_date,
    "Created_Date" as created_date,
    "Is_Latest" as is_latest,
    "Effective_From" as effective_from,
    "Effective_To" as effective_to,
    "Char_8_ASCII_Index" as char_8_ascii_index
from {{ source('ukhfd_ods', 'closed_organisations') }}
