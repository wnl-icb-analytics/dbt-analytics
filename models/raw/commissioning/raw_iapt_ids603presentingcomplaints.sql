{{
    config(
        description="Raw layer (NHS Talking Therapies (IAPT) dataset). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.IAPT.IDS603PresentingComplaints \ndbt: source(''iapt'', ''IDS603PresentingComplaints'') \nColumns:\n  SK -> sk\n  ServiceRequestId -> service_request_id\n  FindSchemeInUse -> find_scheme_in_use\n  PresComp -> pres_comp\n  PresCompCodSig -> pres_comp_cod_sig\n  PresCompDate -> pres_comp_date\n  EFFECTIVE_FROM -> effective_from\n  RecordNumber -> record_number\n  UniqueID_IDS603 -> unique_id_ids603\n  OrgID_Provider -> org_id_provider\n  Person_ID -> person_id\n  UniqueSubmissionID -> unique_submission_id\n  Unique_ServiceRequestID -> unique_service_request_id\n  Validated_PresentingComplaint -> validated_presenting_complaint\n  PresentingComplaintCode_ICD10_Mapped -> presenting_complaint_code_icd10_mapped\n  PresentingComplaintCode_ICD10_Master -> presenting_complaint_code_icd10_master\n  PresentingComplaintDescription_ICD10_Master -> presenting_complaint_description_icd10_master\n  Unique_MonthID -> unique_month_id\n  RecordStartDate -> record_start_date\n  RecordEndDate -> record_end_date\n  PathwayID -> pathway_id\n  PresComp_FSN -> pres_comp_fsn\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicDateAdded -> dmic_date_added"
    )
}}
select
    "SK" as sk,
    "ServiceRequestId" as service_request_id,
    "FindSchemeInUse" as find_scheme_in_use,
    "PresComp" as pres_comp,
    "PresCompCodSig" as pres_comp_cod_sig,
    "PresCompDate" as pres_comp_date,
    "EFFECTIVE_FROM" as effective_from,
    "RecordNumber" as record_number,
    "UniqueID_IDS603" as unique_id_ids603,
    "OrgID_Provider" as org_id_provider,
    "Person_ID" as person_id,
    "UniqueSubmissionID" as unique_submission_id,
    "Unique_ServiceRequestID" as unique_service_request_id,
    "Validated_PresentingComplaint" as validated_presenting_complaint,
    "PresentingComplaintCode_ICD10_Mapped" as presenting_complaint_code_icd10_mapped,
    "PresentingComplaintCode_ICD10_Master" as presenting_complaint_code_icd10_master,
    "PresentingComplaintDescription_ICD10_Master" as presenting_complaint_description_icd10_master,
    "Unique_MonthID" as unique_month_id,
    "RecordStartDate" as record_start_date,
    "RecordEndDate" as record_end_date,
    "PathwayID" as pathway_id,
    "PresComp_FSN" as pres_comp_fsn,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicDateAdded" as dmic_date_added
from {{ source('iapt', 'IDS603PresentingComplaints') }}
