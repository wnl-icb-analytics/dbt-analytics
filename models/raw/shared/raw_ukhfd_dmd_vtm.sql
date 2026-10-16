{{
    config(
        description="Raw layer: Virtual therapeutic moieties independent of strength and form, with UKHFD snapshot history. 1:1 passthrough with cleaned column names. \nSource: UKHFD.DMPlusD.dim_VTM_Virtual_Therapeutic_Moiety_SCD \ndbt: source(''ukhfd_dmd'', ''dmd_vtm'') \nColumns:\n  VTMID -> vtmid\n  INVALID -> invalid\n  NM -> nm\n  ABBREVNM -> abbrevnm\n  VTMIDPREV -> vtmidprev\n  VTMIDDT -> vtmiddt\n  Unique_Column -> unique_column\n  In_Source_Data -> in_source_data\n  Import_Date -> import_date\n  Created_Date -> created_date\n  Is_Latest -> is_latest\n  Effective_From -> effective_from\n  Effective_To -> effective_to"
    )
}}
select
    "VTMID" as vtmid,
    "INVALID" as invalid,
    "NM" as nm,
    "ABBREVNM" as abbrevnm,
    "VTMIDPREV" as vtmidprev,
    "VTMIDDT" as vtmiddt,
    "Unique_Column" as unique_column,
    "In_Source_Data" as in_source_data,
    "Import_Date" as import_date,
    "Created_Date" as created_date,
    "Is_Latest" as is_latest,
    "Effective_From" as effective_from,
    "Effective_To" as effective_to
from {{ source('ukhfd_dmd', 'dmd_vtm') }}
