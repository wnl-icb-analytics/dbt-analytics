{{
    config(
        description="Raw layer: Overall IMD score, rank and decile with the local authority supplied with each release, one row per LSOA and release snapshot.. 1:1 passthrough with cleaned column names. \nSource: UKHFD.Demography.fact_Index_Of_Multiple_Deprivation_By_LSOA \ndbt: source(''ukhfd_demography'', ''index_of_multiple_deprivation_by_lsoa'') \nColumns:\n  LSOA_Code -> lsoa_code\n  IMD_Score -> imd_score\n  IMD_Rank -> imd_rank\n  IMD_Decile -> imd_decile\n  Effective_Snapshot_Date -> effective_snapshot_date\n  DataSourceFileForThisSnapshot_Version -> data_source_file_for_this_snapshot_version\n  Report_Period_Length -> report_period_length\n  Unique_ID -> unique_id\n  AuditKey -> audit_key\n  LA_District_Code -> la_district_code\n  LSOA_Code_2021 -> lsoa_code_2021\n  LAD_Code_2024 -> lad_code_2024\n  LSOA_Code_Year -> lsoa_code_year"
    )
}}
select
    "LSOA_Code" as lsoa_code,
    "IMD_Score" as imd_score,
    "IMD_Rank" as imd_rank,
    "IMD_Decile" as imd_decile,
    "Effective_Snapshot_Date" as effective_snapshot_date,
    "DataSourceFileForThisSnapshot_Version" as data_source_file_for_this_snapshot_version,
    "Report_Period_Length" as report_period_length,
    "Unique_ID" as unique_id,
    "AuditKey" as audit_key,
    "LA_District_Code" as la_district_code,
    "LSOA_Code_2021" as lsoa_code_2021,
    "LAD_Code_2024" as lad_code_2024,
    "LSOA_Code_Year" as lsoa_code_year
from {{ source('ukhfd_demography', 'index_of_multiple_deprivation_by_lsoa') }}
