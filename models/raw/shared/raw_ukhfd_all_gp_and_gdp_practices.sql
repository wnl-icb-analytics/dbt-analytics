{{
    config(
        description="Raw layer: All GP and general dental practices nationally (epraccur-style SCD); Is_Latest marks the current row per organisation.. 1:1 passthrough with cleaned column names. \nSource: UKHFD.ODS.dim_All_GP_and_GDP_Practices_SCD \ndbt: source(''ukhfd_ods'', ''all_gp_and_gdp_practices'') \nColumns:\n  Organisation_Code -> organisation_code\n  Organisation_Name -> organisation_name\n  Postcode -> postcode\n  Open_Date -> open_date\n  Close_Date -> close_date\n  Status_Code -> status_code\n  Parent_Organisation_Code -> parent_organisation_code\n  Prescribing_Setting -> prescribing_setting\n  Is_Latest -> is_latest\n  Effective_From -> effective_from\n  Effective_To -> effective_to"
    )
}}
select
    "Organisation_Code" as organisation_code,
    "Organisation_Name" as organisation_name,
    "Postcode" as postcode,
    "Open_Date" as open_date,
    "Close_Date" as close_date,
    "Status_Code" as status_code,
    "Parent_Organisation_Code" as parent_organisation_code,
    "Prescribing_Setting" as prescribing_setting,
    "Is_Latest" as is_latest,
    "Effective_From" as effective_from,
    "Effective_To" as effective_to
from {{ source('ukhfd_ods', 'all_gp_and_gdp_practices') }}
