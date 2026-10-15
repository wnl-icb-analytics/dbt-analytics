{{
    config(
        description="Raw layer (NHS Talking Therapies (IAPT) dataset). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.IAPT.IDS202CareActivity \ndbt: source(''iapt'', ''IDS202CareActivity'') \nColumns:\n  SK -> sk\n  CareActId -> care_act_id\n  CareContactId -> care_contact_id\n  CarePersLocalId -> care_pers_local_id\n  ClinContactDurOfCareAct -> clin_contact_dur_of_care_act\n  CodeProcAndProcStatus -> code_proc_and_proc_status\n  FindSchemeInUse -> find_scheme_in_use\n  CodeFind -> code_find\n  CodeObs -> code_obs\n  ObsValue -> obs_value\n  UnitMeasure -> unit_measure\n  EFFECTIVE_FROM -> effective_from\n  RecordNumber -> record_number\n  UniqueID_IDS202 -> unique_id_ids202\n  OrgID_Provider -> org_id_provider\n  Person_ID -> person_id\n  UniqueSubmissionID -> unique_submission_id\n  Unique_ServiceRequestID -> unique_service_request_id\n  Unique_CareContactID -> unique_care_contact_id\n  Unique_CareActivityID -> unique_care_activity_id\n  Unique_CarePersonnelID_Local -> unique_care_personnel_id_local\n  Validated_FindingCode -> validated_finding_code\n  FindingCode_ICD10_Mapped -> finding_code_icd10_mapped\n  FindingCode_ICD10_Master -> finding_code_icd10_master\n  FindingDescription_ICD10_Master -> finding_description_icd10_master\n  Unique_MonthID -> unique_month_id\n  PathwayID -> pathway_id\n  CodeProcAndProcStatus_FSN -> code_proc_and_proc_status_fsn\n  CodeFind_FSN -> code_find_fsn\n  CodeObs_FSN -> code_obs_fsn\n  dmicImportLogId -> dmic_import_log_id\n  dmicActivityDate -> dmic_activity_date\n  ServiceRequestId -> service_request_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicDateAdded -> dmic_date_added"
    )
}}
select
    "SK" as sk,
    "CareActId" as care_act_id,
    "CareContactId" as care_contact_id,
    "CarePersLocalId" as care_pers_local_id,
    "ClinContactDurOfCareAct" as clin_contact_dur_of_care_act,
    "CodeProcAndProcStatus" as code_proc_and_proc_status,
    "FindSchemeInUse" as find_scheme_in_use,
    "CodeFind" as code_find,
    "CodeObs" as code_obs,
    "ObsValue" as obs_value,
    "UnitMeasure" as unit_measure,
    "EFFECTIVE_FROM" as effective_from,
    "RecordNumber" as record_number,
    "UniqueID_IDS202" as unique_id_ids202,
    "OrgID_Provider" as org_id_provider,
    "Person_ID" as person_id,
    "UniqueSubmissionID" as unique_submission_id,
    "Unique_ServiceRequestID" as unique_service_request_id,
    "Unique_CareContactID" as unique_care_contact_id,
    "Unique_CareActivityID" as unique_care_activity_id,
    "Unique_CarePersonnelID_Local" as unique_care_personnel_id_local,
    "Validated_FindingCode" as validated_finding_code,
    "FindingCode_ICD10_Mapped" as finding_code_icd10_mapped,
    "FindingCode_ICD10_Master" as finding_code_icd10_master,
    "FindingDescription_ICD10_Master" as finding_description_icd10_master,
    "Unique_MonthID" as unique_month_id,
    "PathwayID" as pathway_id,
    "CodeProcAndProcStatus_FSN" as code_proc_and_proc_status_fsn,
    "CodeFind_FSN" as code_find_fsn,
    "CodeObs_FSN" as code_obs_fsn,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicActivityDate" as dmic_activity_date,
    "ServiceRequestId" as service_request_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicDateAdded" as dmic_date_added
from {{ source('iapt', 'IDS202CareActivity') }}
