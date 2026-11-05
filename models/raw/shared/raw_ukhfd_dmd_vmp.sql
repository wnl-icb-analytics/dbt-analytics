{{
    config(
        description="Raw layer: Generic medicinal products with strength and dose form, with UKHFD snapshot history. 1:1 passthrough with cleaned column names. \nSource: UKHFD.DMPlusD.dim_VMP_Virtual_Medicinal_Products_SCD \ndbt: source(''ukhfd_dmd'', ''dmd_vmp'') \nColumns:\n  VMPS_Id -> vmps_id\n  VPID -> vpid\n  VPIDDT -> vpiddt\n  VPIDPREV -> vpidprev\n  VTMID -> vtmid\n  INVALID -> invalid\n  NM -> nm\n  ABBREVNM -> abbrevnm\n  BASISCD -> basiscd\n  NMDT -> nmdt\n  NMPREV -> nmprev\n  BASIS_PREVCD -> basis_prevcd\n  NMCHANGECD -> nmchangecd\n  COMBPRODCD -> combprodcd\n  PRES_STATCD -> pres_statcd\n  SUG_F -> sug_f\n  GLU_F -> glu_f\n  PRES_F -> pres_f\n  CFC_F -> cfc_f\n  NON_AVAILCD -> non_availcd\n  NON_AVAILDT -> non_availdt\n  DF_INDCD -> df_indcd\n  UDFS -> udfs\n  UDFS_UOMCD -> udfs_uomcd\n  UNIT_DOSE_UOMCD -> unit_dose_uomcd\n  Unique_Column -> unique_column\n  In_Source_Data -> in_source_data\n  Import_Date -> import_date\n  Created_Date -> created_date\n  Is_Latest -> is_latest\n  Effective_From -> effective_from\n  Effective_To -> effective_to"
    )
}}
select
    "VMPS_Id" as vmps_id,
    "VPID" as vpid,
    "VPIDDT" as vpiddt,
    "VPIDPREV" as vpidprev,
    "VTMID" as vtmid,
    "INVALID" as invalid,
    "NM" as nm,
    "ABBREVNM" as abbrevnm,
    "BASISCD" as basiscd,
    "NMDT" as nmdt,
    "NMPREV" as nmprev,
    "BASIS_PREVCD" as basis_prevcd,
    "NMCHANGECD" as nmchangecd,
    "COMBPRODCD" as combprodcd,
    "PRES_STATCD" as pres_statcd,
    "SUG_F" as sug_f,
    "GLU_F" as glu_f,
    "PRES_F" as pres_f,
    "CFC_F" as cfc_f,
    "NON_AVAILCD" as non_availcd,
    "NON_AVAILDT" as non_availdt,
    "DF_INDCD" as df_indcd,
    "UDFS" as udfs,
    "UDFS_UOMCD" as udfs_uomcd,
    "UNIT_DOSE_UOMCD" as unit_dose_uomcd,
    "Unique_Column" as unique_column,
    "In_Source_Data" as in_source_data,
    "Import_Date" as import_date,
    "Created_Date" as created_date,
    "Is_Latest" as is_latest,
    "Effective_From" as effective_from,
    "Effective_To" as effective_to
from {{ source('ukhfd_dmd', 'dmd_vmp') }}
