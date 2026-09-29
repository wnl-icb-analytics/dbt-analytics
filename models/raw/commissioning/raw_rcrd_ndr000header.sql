{{
    config(
        description="Raw layer (Rapid Cancer Registration Dataset (RCRD)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.RCRD.NDR000Header \ndbt: source(''rcrd'', ''NDR000Header'') \nColumns:\n  Version -> version\n  OrgID_Submit -> org_id_submit\n  Received_Date -> received_date\n  Snapshot -> snapshot\n  UniqSubmissionId -> uniq_submission_id\n  File_Type -> file_type\n  ReportingPeriodStartDate -> reporting_period_start_date\n  ReportingPeriodEndDate -> reporting_period_end_date\n  Unique_MonthId -> unique_month_id\n  Total_ndr001 -> total_ndr001\n  TotalRecords -> total_records\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  NDR000_Id -> ndr000_id\n  dmicDateAdded -> dmic_date_added"
    )
}}
select
    "Version" as version,
    "OrgID_Submit" as org_id_submit,
    "Received_Date" as received_date,
    "Snapshot" as snapshot,
    "UniqSubmissionId" as uniq_submission_id,
    "File_Type" as file_type,
    "ReportingPeriodStartDate" as reporting_period_start_date,
    "ReportingPeriodEndDate" as reporting_period_end_date,
    "Unique_MonthId" as unique_month_id,
    "Total_ndr001" as total_ndr001,
    "TotalRecords" as total_records,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "NDR000_Id" as ndr000_id,
    "dmicDateAdded" as dmic_date_added
from {{ source('rcrd', 'NDR000Header') }}
