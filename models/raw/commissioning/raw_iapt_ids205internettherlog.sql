{{
    config(
        description="Raw layer (NHS Talking Therapies (IAPT) dataset). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.IAPT.IDS205InternetTherLog \ndbt: source(''iapt'', ''IDS205InternetTherLog'') \nColumns:\n  SK -> sk\n  ServiceRequestId -> service_request_id\n  StartDateIntEnabledTherLog -> start_date_int_enabled_ther_log\n  EndDateIntEnabledTherLog -> end_date_int_enabled_ther_log\n  IntEnabledTherProg -> int_enabled_ther_prog\n  DurationIntEnabledTher -> duration_int_enabled_ther\n  CarePersLocalId -> care_pers_local_id\n  IntegratedSoftwareInd -> integrated_software_ind\n  EFFECTIVE_FROM -> effective_from\n  RecordNumber -> record_number\n  UniqueID_IDS205 -> unique_id_ids205\n  OrgID_Provider -> org_id_provider\n  Person_ID -> person_id\n  UniqueSubmissionID -> unique_submission_id\n  Unique_ServiceRequestID -> unique_service_request_id\n  Unique_CarePersonnelID_Local -> unique_care_personnel_id_local\n  Unique_MonthID -> unique_month_id\n  PathwayID -> pathway_id\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicDateAdded -> dmic_date_added"
    )
}}
select
    "SK" as sk,
    "ServiceRequestId" as service_request_id,
    "StartDateIntEnabledTherLog" as start_date_int_enabled_ther_log,
    "EndDateIntEnabledTherLog" as end_date_int_enabled_ther_log,
    "IntEnabledTherProg" as int_enabled_ther_prog,
    "DurationIntEnabledTher" as duration_int_enabled_ther,
    "CarePersLocalId" as care_pers_local_id,
    "IntegratedSoftwareInd" as integrated_software_ind,
    "EFFECTIVE_FROM" as effective_from,
    "RecordNumber" as record_number,
    "UniqueID_IDS205" as unique_id_ids205,
    "OrgID_Provider" as org_id_provider,
    "Person_ID" as person_id,
    "UniqueSubmissionID" as unique_submission_id,
    "Unique_ServiceRequestID" as unique_service_request_id,
    "Unique_CarePersonnelID_Local" as unique_care_personnel_id_local,
    "Unique_MonthID" as unique_month_id,
    "PathwayID" as pathway_id,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicDateAdded" as dmic_date_added
from {{ source('iapt', 'IDS205InternetTherLog') }}
