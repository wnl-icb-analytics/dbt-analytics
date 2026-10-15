{{
    config(
        description="Raw layer (NHS Talking Therapies (IAPT) dataset). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.IAPT.IDS012OverseasVisitorChargCat \ndbt: source(''iapt'', ''IDS012OverseasVisitorChargCat'') \nColumns:\n  SK -> sk\n  LocalPatientId -> local_patient_id\n  OvsVisChCat -> ovs_vis_ch_cat\n  OvsVisChCatAppDate -> ovs_vis_ch_cat_app_date\n  EFFECTIVE_FROM -> effective_from\n  RecordNumber -> record_number\n  UniqueID_IDS012 -> unique_id_ids012\n  OrgID_Provider -> org_id_provider\n  Person_ID -> person_id\n  UniqueSubmissionID -> unique_submission_id\n  Unique_MonthID -> unique_month_id\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicDateAdded -> dmic_date_added\n  Unique_LocalPatientId -> unique_local_patient_id"
    )
}}
select
    "SK" as sk,
    "LocalPatientId" as local_patient_id,
    "OvsVisChCat" as ovs_vis_ch_cat,
    "OvsVisChCatAppDate" as ovs_vis_ch_cat_app_date,
    "EFFECTIVE_FROM" as effective_from,
    "RecordNumber" as record_number,
    "UniqueID_IDS012" as unique_id_ids012,
    "OrgID_Provider" as org_id_provider,
    "Person_ID" as person_id,
    "UniqueSubmissionID" as unique_submission_id,
    "Unique_MonthID" as unique_month_id,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicDateAdded" as dmic_date_added,
    "Unique_LocalPatientId" as unique_local_patient_id
from {{ source('iapt', 'IDS012OverseasVisitorChargCat') }}
