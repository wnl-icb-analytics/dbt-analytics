{{
    config(
        description="Raw layer (NHS Talking Therapies (IAPT) dataset). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.IAPT.IDS606CodedScoreAssessmentRefer \ndbt: source(''iapt'', ''IDS606CodedScoreAssessmentRefer'') \nColumns:\n  SK -> sk\n  ServiceRequestId -> service_request_id\n  CodedAssToolType -> coded_ass_tool_type\n  PersScore -> pers_score\n  AssToolCompDate -> ass_tool_comp_date\n  AssToolCompTime -> ass_tool_comp_time\n  EFFECTIVE_FROM -> effective_from\n  RecordNumber -> record_number\n  UniqueID_IDS606 -> unique_id_ids606\n  OrgID_Provider -> org_id_provider\n  Person_ID -> person_id\n  UniqueSubmissionID -> unique_submission_id\n  Unique_ServiceRequestID -> unique_service_request_id\n  Age_AssessmentCompletion_Date -> age_assessment_completion_date\n  Unique_MonthID -> unique_month_id\n  PathwayID -> pathway_id\n  CodedAssToolType_FSN -> coded_ass_tool_type_fsn\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicDateAdded -> dmic_date_added"
    )
}}
select
    "SK" as sk,
    "ServiceRequestId" as service_request_id,
    "CodedAssToolType" as coded_ass_tool_type,
    "PersScore" as pers_score,
    "AssToolCompDate" as ass_tool_comp_date,
    "AssToolCompTime" as ass_tool_comp_time,
    "EFFECTIVE_FROM" as effective_from,
    "RecordNumber" as record_number,
    "UniqueID_IDS606" as unique_id_ids606,
    "OrgID_Provider" as org_id_provider,
    "Person_ID" as person_id,
    "UniqueSubmissionID" as unique_submission_id,
    "Unique_ServiceRequestID" as unique_service_request_id,
    "Age_AssessmentCompletion_Date" as age_assessment_completion_date,
    "Unique_MonthID" as unique_month_id,
    "PathwayID" as pathway_id,
    "CodedAssToolType_FSN" as coded_ass_tool_type_fsn,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicDateAdded" as dmic_date_added
from {{ source('iapt', 'IDS606CodedScoreAssessmentRefer') }}
