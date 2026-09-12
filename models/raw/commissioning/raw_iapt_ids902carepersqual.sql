{{
    config(
        description="Raw layer (NHS Talking Therapies (IAPT) dataset). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.IAPT.IDS902CarePersQual \ndbt: source(''iapt'', ''IDS902CarePersQual'') \nColumns:\n  CarePersLocalId -> care_pers_local_id\n  QualAttainLevelIAPT -> qual_attain_level_iapt\n  QualAwardedDate -> qual_awarded_date\n  QualPlannedCompletionDate -> qual_planned_completion_date\n  EFFECTIVE_FROM -> effective_from\n  UniqueID_IDS902 -> unique_id_ids902\n  OrgID_Provider -> org_id_provider\n  UniqueSubmissionID -> unique_submission_id\n  Unique_CarePersonnelID_Local -> unique_care_personnel_id_local\n  Unique_MonthID -> unique_month_id\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicDateAdded -> dmic_date_added"
    )
}}
select
    "CarePersLocalId" as care_pers_local_id,
    "QualAttainLevelIAPT" as qual_attain_level_iapt,
    "QualAwardedDate" as qual_awarded_date,
    "QualPlannedCompletionDate" as qual_planned_completion_date,
    "EFFECTIVE_FROM" as effective_from,
    "UniqueID_IDS902" as unique_id_ids902,
    "OrgID_Provider" as org_id_provider,
    "UniqueSubmissionID" as unique_submission_id,
    "Unique_CarePersonnelID_Local" as unique_care_personnel_id_local,
    "Unique_MonthID" as unique_month_id,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicDateAdded" as dmic_date_added
from {{ source('iapt', 'IDS902CarePersQual') }}
