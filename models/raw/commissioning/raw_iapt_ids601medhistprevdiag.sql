{{
    config(
        description="Raw layer (NHS Talking Therapies (IAPT) dataset). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.IAPT.IDS601MedHistPrevDiag \ndbt: source(''iapt'', ''IDS601MedHistPrevDiag'') \nColumns:\n  SK -> sk\n  LocalPatientId -> local_patient_id\n  DiagSchemeInUse -> diag_scheme_in_use\n  PrevDiag -> prev_diag\n  DiagDate -> diag_date\n  EFFECTIVE_FROM -> effective_from\n  RecordNumber -> record_number\n  UniqueID_IDS601 -> unique_id_ids601\n  OrgID_Provider -> org_id_provider\n  Person_ID -> person_id\n  UniqueSubmissionID -> unique_submission_id\n  Unique_MonthID -> unique_month_id\n  RecordStartDate -> record_start_date\n  RecordEndDate -> record_end_date\n  PrevDiag_FSN -> prev_diag_fsn\n  PrevDiag_ICD10_Mapped -> prev_diag_icd10_mapped\n  PrevDiag_ICD10_Master -> prev_diag_icd10_master\n  PrevDiagDescription_ICD10_Master -> prev_diag_description_icd10_master\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicDSCRO -> dmic_dscro\n  dmicDateAdded -> dmic_date_added\n  Unique_LocalPatientId -> unique_local_patient_id"
    )
}}
select
    "SK" as sk,
    "LocalPatientId" as local_patient_id,
    "DiagSchemeInUse" as diag_scheme_in_use,
    "PrevDiag" as prev_diag,
    "DiagDate" as diag_date,
    "EFFECTIVE_FROM" as effective_from,
    "RecordNumber" as record_number,
    "UniqueID_IDS601" as unique_id_ids601,
    "OrgID_Provider" as org_id_provider,
    "Person_ID" as person_id,
    "UniqueSubmissionID" as unique_submission_id,
    "Unique_MonthID" as unique_month_id,
    "RecordStartDate" as record_start_date,
    "RecordEndDate" as record_end_date,
    "PrevDiag_FSN" as prev_diag_fsn,
    "PrevDiag_ICD10_Mapped" as prev_diag_icd10_mapped,
    "PrevDiag_ICD10_Master" as prev_diag_icd10_master,
    "PrevDiagDescription_ICD10_Master" as prev_diag_description_icd10_master,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicDSCRO" as dmic_dscro,
    "dmicDateAdded" as dmic_date_added,
    "Unique_LocalPatientId" as unique_local_patient_id
from {{ source('iapt', 'IDS601MedHistPrevDiag') }}
