{{
    config(
        description="Raw layer: English Indices of Deprivation File 7 measures in long form, one row per LSOA, measure and release snapshot.. 1:1 passthrough with cleaned column names. \nSource: UKHFD.Demography.fact_Domains_Of_Deprivation_By_LSOA_V3 \ndbt: source(''ukhfd_demography'', ''domains_of_deprivation_by_lsoa'') \nColumns:\n  LSOA_Code -> lsoa_code\n  LSOA_Code_Year -> lsoa_code_year\n  Measure_Name -> measure_name\n  Measure_Value -> measure_value\n  Measure_Value_Str -> measure_value_str\n  Effective_Snapshot_Date -> effective_snapshot_date\n  DataSourceFileForThisSnapshot_Version -> data_source_file_for_this_snapshot_version\n  Report_Period_Length -> report_period_length\n  Unique_ID -> unique_id\n  AuditKey -> audit_key"
    )
}}
select
    "LSOA_Code" as lsoa_code,
    "LSOA_Code_Year" as lsoa_code_year,
    "Measure_Name" as measure_name,
    "Measure_Value" as measure_value,
    "Measure_Value_Str" as measure_value_str,
    "Effective_Snapshot_Date" as effective_snapshot_date,
    "DataSourceFileForThisSnapshot_Version" as data_source_file_for_this_snapshot_version,
    "Report_Period_Length" as report_period_length,
    "Unique_ID" as unique_id,
    "AuditKey" as audit_key
from {{ source('ukhfd_demography', 'domains_of_deprivation_by_lsoa') }}
