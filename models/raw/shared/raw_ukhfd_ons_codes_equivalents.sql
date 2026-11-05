{{
    config(
        description="Raw layer: ONS geography codes with names, equivalent codes, status and introduction and termination dates.. 1:1 passthrough with cleaned column names. \nSource: UKHFD.ONS_Codes.dim_Equivalents \ndbt: source(''ukhfd_ons_codes'', ''equivalents'') \nColumns:\n  Geography_Code -> geography_code\n  Geography_Name -> geography_name\n  Geography_Name_Welsh -> geography_name_welsh\n  ONS_Geography_Code -> ons_geography_code\n  ONS_Geography_Name -> ons_geography_name\n  DCLG_Geography_Code -> dclg_geography_code\n  DCLG_Geography_Name -> dclg_geography_name\n  DH_Geography_Code -> dh_geography_code\n  DH_Geography_Name -> dh_geography_name\n  Scottish_Geography_Code -> scottish_geography_code\n  Scottish_Geography_Name -> scottish_geography_name\n  NI_Geography_Code -> ni_geography_code\n  NI_Geography_Name -> ni_geography_name\n  WG_Geography_Code -> wg_geography_code\n  WG_Geography_Name -> wg_geography_name\n  WG_Geography_Name_Welsh -> wg_geography_name_welsh\n  Date_Of_Introduction -> date_of_introduction\n  Date_Of_Termination -> date_of_termination\n  Entity_Code -> entity_code\n  Year -> year\n  Status -> status\n  Import_Date -> import_date\n  Created_Date -> created_date"
    )
}}
select
    "Geography_Code" as geography_code,
    "Geography_Name" as geography_name,
    "Geography_Name_Welsh" as geography_name_welsh,
    "ONS_Geography_Code" as ons_geography_code,
    "ONS_Geography_Name" as ons_geography_name,
    "DCLG_Geography_Code" as dclg_geography_code,
    "DCLG_Geography_Name" as dclg_geography_name,
    "DH_Geography_Code" as dh_geography_code,
    "DH_Geography_Name" as dh_geography_name,
    "Scottish_Geography_Code" as scottish_geography_code,
    "Scottish_Geography_Name" as scottish_geography_name,
    "NI_Geography_Code" as ni_geography_code,
    "NI_Geography_Name" as ni_geography_name,
    "WG_Geography_Code" as wg_geography_code,
    "WG_Geography_Name" as wg_geography_name,
    "WG_Geography_Name_Welsh" as wg_geography_name_welsh,
    "Date_Of_Introduction" as date_of_introduction,
    "Date_Of_Termination" as date_of_termination,
    "Entity_Code" as entity_code,
    "Year" as year,
    "Status" as status,
    "Import_Date" as import_date,
    "Created_Date" as created_date
from {{ source('ukhfd_ons_codes', 'equivalents') }}
