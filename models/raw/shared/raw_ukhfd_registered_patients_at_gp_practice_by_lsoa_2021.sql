{{
    config(
        description="Raw layer: Registered patient counts by GP Practice, sex and 2021 LSOA per monthly snapshot. 1:1 passthrough with cleaned column names. \nSource: UKHFD.Demography.fact_No_Of_Patients_Regd_At_GP_Practice_LSOA_2021_Level \ndbt: source(''ukhfd_demography'', ''registered_patients_at_gp_practice_by_lsoa_2021'') \nColumns:\n  GP_Practice_Code -> gp_practice_code\n  LSOA_Code -> lsoa_code\n  Census_Year -> census_year\n  Sex -> sex\n  Size -> size\n  Effective_Snapshot_Date -> effective_snapshot_date\n  DataSourceFileForThisSnapshot_Version -> data_source_file_for_this_snapshot_version\n  Report_Period_Length -> report_period_length\n  Unique_ID -> unique_id\n  AuditKey -> audit_key"
    )
}}
select
    "GP_Practice_Code" as gp_practice_code,
    "LSOA_Code" as lsoa_code,
    "Census_Year" as census_year,
    "Sex" as sex,
    "Size" as size,
    "Effective_Snapshot_Date" as effective_snapshot_date,
    "DataSourceFileForThisSnapshot_Version" as data_source_file_for_this_snapshot_version,
    "Report_Period_Length" as report_period_length,
    "Unique_ID" as unique_id,
    "AuditKey" as audit_key
from {{ source('ukhfd_demography', 'registered_patients_at_gp_practice_by_lsoa_2021') }}
