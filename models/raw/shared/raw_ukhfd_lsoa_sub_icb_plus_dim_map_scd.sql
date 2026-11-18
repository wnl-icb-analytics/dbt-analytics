{{
    config(
        description="Raw layer (Mapping between LSOA and Sub-ICB including higher NHSE geographies). 1:1 passthrough with cleaned column names. \nSource: UKHFD.LSOA_Sub_ICB_ICB_NHSER_LAD.dim_Map_SCD \ndbt: source(''ukhfd_lsoa_to_sub_icb_plus'', ''dim_Map_SCD'') \nColumns:\n  Object_ID -> object_id\n  LSOA_Code -> lsoa_code\n  LSOA_Code_Year -> lsoa_code_year\n  Sub_ICB_Code -> sub_icb_code\n  Sub_ICB_Code_Year -> sub_icb_code_year\n  Sub_ICB_ODS_Code -> sub_icb_ods_code\n  Sub_ICB_ODS_Code_Year -> sub_icb_ods_code_year\n  ICB_Code -> icb_code\n  ICB_Code_Year -> icb_code_year\n  ICB_ODS_Code -> icb_ods_code\n  ICB_ODS_Code_Year -> icb_ods_code_year\n  NHSER_Code -> nhser_code\n  NHSER_Code_Year -> nhser_code_year\n  NHSER_ODS_Code -> nhser_ods_code\n  NHSER_ODS_Code_Year -> nhser_ods_code_year\n  LAD_Code -> lad_code\n  LAD_Code_Year -> lad_code_year\n  In_Source_Data -> in_source_data\n  Effective_Snapshot_Date -> effective_snapshot_date\n  Is_Latest -> is_latest\n  Effective_From -> effective_from\n  Effective_To -> effective_to"
    )
}}
select
    "Object_ID" as object_id,
    "LSOA_Code" as lsoa_code,
    "LSOA_Code_Year" as lsoa_code_year,
    "Sub_ICB_Code" as sub_icb_code,
    "Sub_ICB_Code_Year" as sub_icb_code_year,
    "Sub_ICB_ODS_Code" as sub_icb_ods_code,
    "Sub_ICB_ODS_Code_Year" as sub_icb_ods_code_year,
    "ICB_Code" as icb_code,
    "ICB_Code_Year" as icb_code_year,
    "ICB_ODS_Code" as icb_ods_code,
    "ICB_ODS_Code_Year" as icb_ods_code_year,
    "NHSER_Code" as nhser_code,
    "NHSER_Code_Year" as nhser_code_year,
    "NHSER_ODS_Code" as nhser_ods_code,
    "NHSER_ODS_Code_Year" as nhser_ods_code_year,
    "LAD_Code" as lad_code,
    "LAD_Code_Year" as lad_code_year,
    "In_Source_Data" as in_source_data,
    "Effective_Snapshot_Date" as effective_snapshot_date,
    "Is_Latest" as is_latest,
    "Effective_From" as effective_from,
    "Effective_To" as effective_to
from {{ source('ukhfd_lsoa_to_sub_icb_plus', 'dim_Map_SCD') }}
