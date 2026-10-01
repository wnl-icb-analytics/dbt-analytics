{{
    config(
        description="Raw layer: Practice-level need indices and weighted populations by allocation base year.. 1:1 passthrough with cleaned column names. \nSource: UKHFD.Comm_Allocations.fact_Weighted_Regs_By_GP_Practice_Base \ndbt: source(''ukhfd_comm_allocations'', ''weighted_regs_by_gp_practice_base'') \nColumns:\n  Title -> title\n  GP_Practice_Code -> gp_practice_code\n  Metric_Name -> metric_name\n  Metric_Value -> metric_value\n  Metric_Value_Str -> metric_value_str\n  Effective_Snapshot_Date -> effective_snapshot_date\n  DataSourceFileForThisSnapshot_Version -> data_source_file_for_this_snapshot_version\n  Report_Period_Length -> report_period_length\n  Unique_ID -> unique_id\n  AuditKey -> audit_key"
    )
}}
select
    "Title" as title,
    "GP_Practice_Code" as gp_practice_code,
    "Metric_Name" as metric_name,
    "Metric_Value" as metric_value,
    "Metric_Value_Str" as metric_value_str,
    "Effective_Snapshot_Date" as effective_snapshot_date,
    "DataSourceFileForThisSnapshot_Version" as data_source_file_for_this_snapshot_version,
    "Report_Period_Length" as report_period_length,
    "Unique_ID" as unique_id,
    "AuditKey" as audit_key
from {{ source('ukhfd_comm_allocations', 'weighted_regs_by_gp_practice_base') }}
