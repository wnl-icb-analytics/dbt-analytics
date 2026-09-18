{{
    config(
        description="Raw layer (NHS Talking Therapies (IAPT) dataset). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.IAPT.IDS602LongTermCondition \ndbt: source(''iapt'', ''IDS602LongTermCondition'') \nColumns:\n  SK -> sk\n  ServiceRequestId -> service_request_id\n  FindSchemeInUse -> find_scheme_in_use\n  LongTermCondition -> long_term_condition\n  EFFECTIVE_FROM -> effective_from\n  RecordNumber -> record_number\n  UniqueID_IDS602 -> unique_id_ids602\n  OrgID_Provider -> org_id_provider\n  Person_ID -> person_id\n  UniqueSubmissionID -> unique_submission_id\n  Unique_ServiceRequestID -> unique_service_request_id\n  Validated_LongTermConditionCode -> validated_long_term_condition_code\n  LongTermConditionCode_ICD10_Mapped -> long_term_condition_code_icd10_mapped\n  LongTermConditionCode_ICD10_Master -> long_term_condition_code_icd10_master\n  LongTermConditionDescription_ICD10_Master -> long_term_condition_description_icd10_master\n  Unique_MonthID -> unique_month_id\n  RecordStartDate -> record_start_date\n  RecordEndDate -> record_end_date\n  PathwayID -> pathway_id\n  LongTermCondition_FSN -> long_term_condition_fsn\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicDateAdded -> dmic_date_added"
    )
}}
select
    "SK" as sk,
    "ServiceRequestId" as service_request_id,
    "FindSchemeInUse" as find_scheme_in_use,
    "LongTermCondition" as long_term_condition,
    "EFFECTIVE_FROM" as effective_from,
    "RecordNumber" as record_number,
    "UniqueID_IDS602" as unique_id_ids602,
    "OrgID_Provider" as org_id_provider,
    "Person_ID" as person_id,
    "UniqueSubmissionID" as unique_submission_id,
    "Unique_ServiceRequestID" as unique_service_request_id,
    "Validated_LongTermConditionCode" as validated_long_term_condition_code,
    "LongTermConditionCode_ICD10_Mapped" as long_term_condition_code_icd10_mapped,
    "LongTermConditionCode_ICD10_Master" as long_term_condition_code_icd10_master,
    "LongTermConditionDescription_ICD10_Master" as long_term_condition_description_icd10_master,
    "Unique_MonthID" as unique_month_id,
    "RecordStartDate" as record_start_date,
    "RecordEndDate" as record_end_date,
    "PathwayID" as pathway_id,
    "LongTermCondition_FSN" as long_term_condition_fsn,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicDateAdded" as dmic_date_added
from {{ source('iapt', 'IDS602LongTermCondition') }}
