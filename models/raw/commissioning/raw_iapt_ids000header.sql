{{
    config(
        description="Raw layer (NHS Talking Therapies (IAPT) dataset). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.IAPT.IDS000Header \ndbt: source(''iapt'', ''IDS000Header'') \nColumns:\n  DatSetVer -> dat_set_ver\n  OrgIDProv -> org_id_prov\n  OrgIDSubmit -> org_id_submit\n  PrimSystemInUse -> prim_system_in_use\n  ReportingPeriodStartDate -> reporting_period_start_date\n  ReportingPeriodEndDate -> reporting_period_end_date\n  DateTimeDatSetCreate -> date_time_dat_set_create\n  EFFECTIVE_FROM -> effective_from\n  UniqueID_IDS000 -> unique_id_ids000\n  File_Type -> file_type\n  TotalRecords -> total_records\n  UniqueSubmissionID -> unique_submission_id\n  Total_IDS001 -> total_ids001\n  Total_IDS002 -> total_ids002\n  Total_IDS004 -> total_ids004\n  Total_IDS007 -> total_ids007\n  Total_IDS011 -> total_ids011\n  Total_IDS012 -> total_ids012\n  Total_IDS101 -> total_ids101\n  Total_IDS105 -> total_ids105\n  Total_IDS108 -> total_ids108\n  Total_IDS201 -> total_ids201\n  Total_IDS202 -> total_ids202\n  Total_IDS205 -> total_ids205\n  Total_IDS602 -> total_ids602\n  Total_IDS603 -> total_ids603\n  Total_IDS606 -> total_ids606\n  Total_IDS607 -> total_ids607\n  Total_IDS803 -> total_ids803\n  Total_IDS902 -> total_ids902\n  Unique_MonthID -> unique_month_id\n  Total_IDS003 -> total_ids003\n  Total_IDS601 -> total_ids601\n  dmicImportLogId -> dmic_import_log_id\n  dmicMonthId -> dmic_month_id\n  dmicSystemId -> dmic_system_id\n  dmicDateAdded -> dmic_date_added"
    )
}}
select
    "DatSetVer" as dat_set_ver,
    "OrgIDProv" as org_id_prov,
    "OrgIDSubmit" as org_id_submit,
    "PrimSystemInUse" as prim_system_in_use,
    "ReportingPeriodStartDate" as reporting_period_start_date,
    "ReportingPeriodEndDate" as reporting_period_end_date,
    "DateTimeDatSetCreate" as date_time_dat_set_create,
    "EFFECTIVE_FROM" as effective_from,
    "UniqueID_IDS000" as unique_id_ids000,
    "File_Type" as file_type,
    "TotalRecords" as total_records,
    "UniqueSubmissionID" as unique_submission_id,
    "Total_IDS001" as total_ids001,
    "Total_IDS002" as total_ids002,
    "Total_IDS004" as total_ids004,
    "Total_IDS007" as total_ids007,
    "Total_IDS011" as total_ids011,
    "Total_IDS012" as total_ids012,
    "Total_IDS101" as total_ids101,
    "Total_IDS105" as total_ids105,
    "Total_IDS108" as total_ids108,
    "Total_IDS201" as total_ids201,
    "Total_IDS202" as total_ids202,
    "Total_IDS205" as total_ids205,
    "Total_IDS602" as total_ids602,
    "Total_IDS603" as total_ids603,
    "Total_IDS606" as total_ids606,
    "Total_IDS607" as total_ids607,
    "Total_IDS803" as total_ids803,
    "Total_IDS902" as total_ids902,
    "Unique_MonthID" as unique_month_id,
    "Total_IDS003" as total_ids003,
    "Total_IDS601" as total_ids601,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicMonthId" as dmic_month_id,
    "dmicSystemId" as dmic_system_id,
    "dmicDateAdded" as dmic_date_added
from {{ source('iapt', 'IDS000Header') }}
