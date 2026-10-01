{{
    config(
        description="Raw layer: Registered patient counts by organisation (GP practice, PCN, ICB and other geographies), sex and 5-year age band per monthly snapshot.. 1:1 passthrough with cleaned column names. \nSource: UKHFD.Demography.fact_No_Of_Patients_Regd_At_GP_Prac_Regions_5Yr_AgeBand \ndbt: source(''ukhfd_demography'', ''registered_patients_by_gp_practice'') \nColumns:\n  Org_Type -> org_type\n  Org_Code -> org_code\n  ONS_Code -> ons_code\n  Sex -> sex\n  Age_Band -> age_band\n  Number_Of_Patients -> number_of_patients\n  Effective_Snapshot_Date -> effective_snapshot_date\n  DataSourceFileForThisSnapshot_Version -> data_source_file_for_this_snapshot_version\n  Report_Period_Length -> report_period_length\n  Unique_ID -> unique_id\n  AuditKey -> audit_key"
    )
}}
select
    "Org_Type" as org_type,
    "Org_Code" as org_code,
    "ONS_Code" as ons_code,
    "Sex" as sex,
    "Age_Band" as age_band,
    "Number_Of_Patients" as number_of_patients,
    "Effective_Snapshot_Date" as effective_snapshot_date,
    "DataSourceFileForThisSnapshot_Version" as data_source_file_for_this_snapshot_version,
    "Report_Period_Length" as report_period_length,
    "Unique_ID" as unique_id,
    "AuditKey" as audit_key
from {{ source('ukhfd_demography', 'registered_patients_by_gp_practice') }}
