{{
    config(
        description="Raw layer (NHS Talking Therapies (IAPT) dataset). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.IAPT.IDS803CareCluster \ndbt: source(''iapt'', ''IDS803CareCluster'') \nColumns:\n  SK -> sk\n  LocalPatientId -> local_patient_id\n  AMHCareClustCodeFin -> amh_care_clust_code_fin\n  StartDateCareClust -> start_date_care_clust\n  StartTimeCareClust -> start_time_care_clust\n  EndDateCareClust -> end_date_care_clust\n  EndTimeCareClust -> end_time_care_clust\n  EFFECTIVE_FROM -> effective_from\n  RecordNumber -> record_number\n  UniqueID_IDS803 -> unique_id_ids803\n  OrgID_Provider -> org_id_provider\n  Person_ID -> person_id\n  UniqueSubmissionID -> unique_submission_id\n  CareCluster_StartedInRP_Flag -> care_cluster_started_in_rp_flag\n  CareCluster_EndedInRP_Flag -> care_cluster_ended_in_rp_flag\n  CareCluster_OpenAtRPEnd_Flag -> care_cluster_open_at_rp_end_flag\n  CareCluster_DayCount -> care_cluster_day_count\n  Unique_MonthID -> unique_month_id\n  RecordStartDate -> record_start_date\n  RecordEndDate -> record_end_date\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicDateAdded -> dmic_date_added\n  Unique_LocalPatientId -> unique_local_patient_id"
    )
}}
select
    "SK" as sk,
    "LocalPatientId" as local_patient_id,
    "AMHCareClustCodeFin" as amh_care_clust_code_fin,
    "StartDateCareClust" as start_date_care_clust,
    "StartTimeCareClust" as start_time_care_clust,
    "EndDateCareClust" as end_date_care_clust,
    "EndTimeCareClust" as end_time_care_clust,
    "EFFECTIVE_FROM" as effective_from,
    "RecordNumber" as record_number,
    "UniqueID_IDS803" as unique_id_ids803,
    "OrgID_Provider" as org_id_provider,
    "Person_ID" as person_id,
    "UniqueSubmissionID" as unique_submission_id,
    "CareCluster_StartedInRP_Flag" as care_cluster_started_in_rp_flag,
    "CareCluster_EndedInRP_Flag" as care_cluster_ended_in_rp_flag,
    "CareCluster_OpenAtRPEnd_Flag" as care_cluster_open_at_rp_end_flag,
    "CareCluster_DayCount" as care_cluster_day_count,
    "Unique_MonthID" as unique_month_id,
    "RecordStartDate" as record_start_date,
    "RecordEndDate" as record_end_date,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicDateAdded" as dmic_date_added,
    "Unique_LocalPatientId" as unique_local_patient_id
from {{ source('iapt', 'IDS803CareCluster') }}
