{{
    config(
        description="Raw layer (NHS Talking Therapies (IAPT) dataset). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.IAPT.IDS105OnwardReferral \ndbt: source(''iapt'', ''IDS105OnwardReferral'') \nColumns:\n  SK -> sk\n  ServiceRequestId -> service_request_id\n  OnwardReferDate -> onward_refer_date\n  OnwardReferTime -> onward_refer_time\n  OnwardReferReason -> onward_refer_reason\n  OrgIDReceiving -> org_id_receiving\n  EFFECTIVE_FROM -> effective_from\n  RecordNumber -> record_number\n  UniqueID_IDS105 -> unique_id_ids105\n  OrgID_Provider -> org_id_provider\n  Person_ID -> person_id\n  UniqueSubmissionID -> unique_submission_id\n  Unique_ServiceRequestID -> unique_service_request_id\n  Unique_MonthID -> unique_month_id\n  PathwayID -> pathway_id\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicDateAdded -> dmic_date_added"
    )
}}
select
    "SK" as sk,
    "ServiceRequestId" as service_request_id,
    "OnwardReferDate" as onward_refer_date,
    "OnwardReferTime" as onward_refer_time,
    "OnwardReferReason" as onward_refer_reason,
    "OrgIDReceiving" as org_id_receiving,
    "EFFECTIVE_FROM" as effective_from,
    "RecordNumber" as record_number,
    "UniqueID_IDS105" as unique_id_ids105,
    "OrgID_Provider" as org_id_provider,
    "Person_ID" as person_id,
    "UniqueSubmissionID" as unique_submission_id,
    "Unique_ServiceRequestID" as unique_service_request_id,
    "Unique_MonthID" as unique_month_id,
    "PathwayID" as pathway_id,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicDateAdded" as dmic_date_added
from {{ source('iapt', 'IDS105OnwardReferral') }}
