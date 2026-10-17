{{
    config(
        description="Raw layer: Active ingredient substances and strengths for virtual products, with UKHFD snapshot history. 1:1 passthrough with cleaned column names. \nSource: UKHFD.DMPlusD.dim_VMP_Virtual_Product_Ingredient_SCD \ndbt: source(''ukhfd_dmd'', ''dmd_vmp_ingredient'') \nColumns:\n  VPID -> vpid\n  ISID -> isid\n  BASIS_STRNTCD -> basis_strntcd\n  BS_SUBID -> bs_subid\n  STRNT_NMRTR_VAL -> strnt_nmrtr_val\n  STRNT_NMRTR_UOMCD -> strnt_nmrtr_uomcd\n  STRNT_DNMTR_VAL -> strnt_dnmtr_val\n  STRNT_DNMTR_UOMCD -> strnt_dnmtr_uomcd\n  Unique_Column -> unique_column\n  In_Source_Data -> in_source_data\n  Import_Date -> import_date\n  Created_Date -> created_date\n  Is_Latest -> is_latest\n  Effective_From -> effective_from\n  Effective_To -> effective_to"
    )
}}
select
    "VPID" as vpid,
    "ISID" as isid,
    "BASIS_STRNTCD" as basis_strntcd,
    "BS_SUBID" as bs_subid,
    "STRNT_NMRTR_VAL" as strnt_nmrtr_val,
    "STRNT_NMRTR_UOMCD" as strnt_nmrtr_uomcd,
    "STRNT_DNMTR_VAL" as strnt_dnmtr_val,
    "STRNT_DNMTR_UOMCD" as strnt_dnmtr_uomcd,
    "Unique_Column" as unique_column,
    "In_Source_Data" as in_source_data,
    "Import_Date" as import_date,
    "Created_Date" as created_date,
    "Is_Latest" as is_latest,
    "Effective_From" as effective_from,
    "Effective_To" as effective_to
from {{ source('ukhfd_dmd', 'dmd_vmp_ingredient') }}
