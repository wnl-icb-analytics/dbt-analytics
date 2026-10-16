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
