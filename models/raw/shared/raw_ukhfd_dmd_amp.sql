{{
    config(
        description="Raw layer: Branded or supplier-specific medicinal products, with UKHFD snapshot history. 1:1 passthrough with cleaned column names. \nSource: UKHFD.DMPlusD.dim_AMP_Actual_Medicinal_Products_SCD \ndbt: source(''ukhfd_dmd'', ''dmd_amp'') \nColumns:\n  AMPS_Id -> amps_id\n  APID -> apid\n  INVALID -> invalid\n  VPID -> vpid\n  NM -> nm\n  ABBREVNM -> abbrevnm\n  DESC -> desc\n  NMDT -> nmdt\n  NM_PREV -> nm_prev\n  SUPPCD -> suppcd\n  LIC_AUTHCD -> lic_authcd\n  LIC_AUTH_PREVCD -> lic_auth_prevcd\n  LIC_AUTHCHANGECD -> lic_authchangecd\n  LIC_AUTHCHANGEDT -> lic_authchangedt\n  COMBPRODCD -> combprodcd\n  FLAVOURCD -> flavourcd\n  CSM -> csm\n  PARALLEL_IMPORT -> parallel_import\n  AVAIL_RESTRICTCD -> avail_restrictcd\n  Unique_Column -> unique_column\n  In_Source_Data -> in_source_data\n  Import_Date -> import_date\n  Created_Date -> created_date\n  Is_Latest -> is_latest\n  Effective_From -> effective_from\n  Effective_To -> effective_to"
    )
}}
select
    "AMPS_Id" as amps_id,
    "APID" as apid,
    "INVALID" as invalid,
    "VPID" as vpid,
    "NM" as nm,
    "ABBREVNM" as abbrevnm,
    "DESC" as desc,
    "NMDT" as nmdt,
    "NM_PREV" as nm_prev,
    "SUPPCD" as suppcd,
    "LIC_AUTHCD" as lic_authcd,
    "LIC_AUTH_PREVCD" as lic_auth_prevcd,
    "LIC_AUTHCHANGECD" as lic_authchangecd,
    "LIC_AUTHCHANGEDT" as lic_authchangedt,
    "COMBPRODCD" as combprodcd,
    "FLAVOURCD" as flavourcd,
    "CSM" as csm,
    "PARALLEL_IMPORT" as parallel_import,
    "AVAIL_RESTRICTCD" as avail_restrictcd,
    "Unique_Column" as unique_column,
    "In_Source_Data" as in_source_data,
    "Import_Date" as import_date,
    "Created_Date" as created_date,
    "Is_Latest" as is_latest,
    "Effective_From" as effective_from,
    "Effective_To" as effective_to
from {{ source('ukhfd_dmd', 'dmd_amp') }}
