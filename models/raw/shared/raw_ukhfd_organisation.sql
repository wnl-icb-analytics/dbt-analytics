{{
    config(
        description="Raw layer: Full ODS organisation and site register, including historical definitions and closed organisations.. 1:1 passthrough with cleaned column names. \nSource: UKHFD.ODS_API.dim_Organisation_SCD \ndbt: source(''ukhfd_ods_api'', ''organisation'') \nColumns:\n  register_id -> register_id\n  Name -> name\n  OrgId_Root -> org_id_root\n  OrgId_assigningAuthorityName -> org_id_assigning_authority_name\n  OrgId_extension -> org_id_extension\n  OrgRecordClass -> org_record_class\n  Status -> status\n  AddrLn1 -> addr_ln1\n  AddrLn2 -> addr_ln2\n  AddrLn3 -> addr_ln3\n  Town -> town\n  County -> county\n  PostCode -> post_code\n  Country -> country\n  UPRN -> uprn\n  In_Source_Data -> in_source_data\n  LastChangeDate -> last_change_date\n  Is_Latest -> is_latest\n  Effective_From -> effective_from\n  Effective_To -> effective_to\n  Char_8_ASCII_Index -> char_8_ascii_index"
    )
}}
select
    "register_id" as register_id,
    "Name" as name,
    "OrgId_Root" as org_id_root,
    "OrgId_assigningAuthorityName" as org_id_assigning_authority_name,
    "OrgId_extension" as org_id_extension,
    "OrgRecordClass" as org_record_class,
    "Status" as status,
    "AddrLn1" as addr_ln1,
    "AddrLn2" as addr_ln2,
    "AddrLn3" as addr_ln3,
    "Town" as town,
    "County" as county,
    "PostCode" as post_code,
    "Country" as country,
    "UPRN" as uprn,
    "In_Source_Data" as in_source_data,
    "LastChangeDate" as last_change_date,
    "Is_Latest" as is_latest,
    "Effective_From" as effective_from,
    "Effective_To" as effective_to,
    "Char_8_ASCII_Index" as char_8_ascii_index
from {{ source('ukhfd_ods_api', 'organisation') }}
