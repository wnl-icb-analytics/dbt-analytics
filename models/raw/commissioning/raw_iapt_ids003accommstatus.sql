{{
    config(
        description="Raw layer (NHS Talking Therapies (IAPT) dataset). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.IAPT.IDS003AccommStatus \ndbt: source(''iapt'', ''IDS003AccommStatus'') \nColumns:\n  SK -> sk\n  LocalPatientId -> local_patient_id\n  AccommodationType -> accommodation_type\n  SettledAccommodationInd -> settled_accommodation_ind\n  AccommodationTypeDate -> accommodation_type_date\n  AccommodationTypeStartDate -> accommodation_type_start_date\n  AccommodationTypeEndDate -> accommodation_type_end_date\n  EFFECTIVE_FROM -> effective_from\n  RecordNumber -> record_number\n  UniqueID_IDS003 -> unique_id_ids003\n  OrgID_Provider -> org_id_provider\n  Person_ID -> person_id\n  UniqueSubmissionID -> unique_submission_id\n  Unique_MonthID -> unique_month_id\n  RecordStartDate -> record_start_date\n  RecordEndDate -> record_end_date\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicDSCRO -> dmic_dscro\n  dmicDateAdded -> dmic_date_added\n  Unique_LocalPatientId -> unique_local_patient_id"
    )
}}
select
    "SK" as sk,
    "LocalPatientId" as local_patient_id,
    "AccommodationType" as accommodation_type,
    "SettledAccommodationInd" as settled_accommodation_ind,
    "AccommodationTypeDate" as accommodation_type_date,
    "AccommodationTypeStartDate" as accommodation_type_start_date,
    "AccommodationTypeEndDate" as accommodation_type_end_date,
    "EFFECTIVE_FROM" as effective_from,
    "RecordNumber" as record_number,
    "UniqueID_IDS003" as unique_id_ids003,
    "OrgID_Provider" as org_id_provider,
    "Person_ID" as person_id,
    "UniqueSubmissionID" as unique_submission_id,
    "Unique_MonthID" as unique_month_id,
    "RecordStartDate" as record_start_date,
    "RecordEndDate" as record_end_date,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicDSCRO" as dmic_dscro,
    "dmicDateAdded" as dmic_date_added,
    "Unique_LocalPatientId" as unique_local_patient_id
from {{ source('iapt', 'IDS003AccommStatus') }}
