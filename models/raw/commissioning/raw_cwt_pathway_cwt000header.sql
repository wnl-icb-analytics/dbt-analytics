{{
    config(
        description="Raw layer (Cancer waiting times data, pathway level version.). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.CWT.CWT000Header \ndbt: source(''cwt_pathway'', ''CWT000Header'') \nColumns:\n  Version -> version\n  OrgID_Provider -> org_id_provider\n  UniqSubmissionID -> uniq_submission_id\n  cwt000_ID -> cwt000_id\n  File_Type -> file_type\n  RP_StartDate -> rp_start_date\n  RP_EndDate -> rp_end_date\n  Unique_MonthID -> unique_month_id\n  Total_cwt001 -> total_cwt001\n  TotalRecords -> total_records\n  dmicImportLogId -> dmic_import_log_id\n  dmicMonthId -> dmic_month_id\n  dmicSystemId -> dmic_system_id\n  dmicDateAdded -> dmic_date_added"
    )
}}
select
    "Version" as version,
    "OrgID_Provider" as org_id_provider,
    "UniqSubmissionID" as uniq_submission_id,
    "cwt000_ID" as cwt000_id,
    "File_Type" as file_type,
    "RP_StartDate" as rp_start_date,
    "RP_EndDate" as rp_end_date,
    "Unique_MonthID" as unique_month_id,
    "Total_cwt001" as total_cwt001,
    "TotalRecords" as total_records,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicMonthId" as dmic_month_id,
    "dmicSystemId" as dmic_system_id,
    "dmicDateAdded" as dmic_date_added
from {{ source('cwt_pathway', 'CWT000Header') }}
