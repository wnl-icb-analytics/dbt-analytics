{{
    config(
        description="Raw layer: ODS epraccur file of GP practices and prescribing cost centres in England and Wales (SCD); Is_Latest marks the current row per organisation.. 1:1 passthrough with cleaned column names. \nSource: UKHFD.ODS.dim_GP_Practices_And_Prescribing_CCs_SCD \ndbt: source(''ukhfd_ods'', ''gp_practices_and_prescribing_ccs'') \nColumns:\n  Organisation_Code -> organisation_code\n  Organisation_Name -> organisation_name\n  National_Grouping_Code -> national_grouping_code\n  High_Level_Health_Authority_Code -> high_level_health_authority_code\n  Address_Line_1 -> address_line_1\n  Address_Line_2 -> address_line_2\n  Address_Line_3 -> address_line_3\n  Address_Line_4 -> address_line_4\n  Address_Line_5 -> address_line_5\n  Postcode -> postcode\n  Open_Date -> open_date\n  Close_Date -> close_date\n  Status_Code -> status_code\n  Parent_Organisation_Code -> parent_organisation_code\n  Join_Parent_Date -> join_parent_date\n  Left_Parent_Date -> left_parent_date\n  Contact_Telephone_Number -> contact_telephone_number\n  Prescribing_Setting -> prescribing_setting\n  Is_Latest -> is_latest\n  Effective_From -> effective_from\n  Effective_To -> effective_to"
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
    "Status_Code" as status_code,
    "Parent_Organisation_Code" as parent_organisation_code,
    "Join_Parent_Date" as join_parent_date,
    "Left_Parent_Date" as left_parent_date,
    "Contact_Telephone_Number" as contact_telephone_number,
    "Prescribing_Setting" as prescribing_setting,
    "Is_Latest" as is_latest,
    "Effective_From" as effective_from,
    "Effective_To" as effective_to
from {{ source('ukhfd_ods', 'gp_practices_and_prescribing_ccs') }}
