{{
    config(
        description="Raw layer (NHS Talking Therapies (IAPT) dataset). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.IAPT.IDS004EmpStatus \ndbt: source(''iapt'', ''IDS004EmpStatus'') \nColumns:\n  SK -> sk\n  LocalPatientId -> local_patient_id\n  EmployStatus -> employ_status\n  EmployStatusRecDate -> employ_status_rec_date\n  WeekHoursWorked -> week_hours_worked\n  SelfEmployInd -> self_employ_ind\n  SickAbsenceInd -> sick_absence_ind\n  SSPInd -> ssp_ind\n  BenefitRecInd -> benefit_rec_ind\n  JSAInd -> jsa_ind\n  ESAInd -> esa_ind\n  UCInd -> uc_ind\n  PIPInd -> pip_ind\n  OtherBenefitInd -> other_benefit_ind\n  EmpSupportInd -> emp_support_ind\n  EmpSupportReferral -> emp_support_referral\n  EmpSupportDischargeDate -> emp_support_discharge_date\n  EFFECTIVE_FROM -> effective_from\n  RecordNumber -> record_number\n  UniqueID_IDS004 -> unique_id_ids004\n  OrgID_Provider -> org_id_provider\n  Person_ID -> person_id\n  UniqueSubmissionID -> unique_submission_id\n  Unique_MonthID -> unique_month_id\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicDateAdded -> dmic_date_added\n  Unique_LocalPatientId -> unique_local_patient_id"
    )
}}
select
    "SK" as sk,
    "LocalPatientId" as local_patient_id,
    "EmployStatus" as employ_status,
    "EmployStatusRecDate" as employ_status_rec_date,
    "WeekHoursWorked" as week_hours_worked,
    "SelfEmployInd" as self_employ_ind,
    "SickAbsenceInd" as sick_absence_ind,
    "SSPInd" as ssp_ind,
    "BenefitRecInd" as benefit_rec_ind,
    "JSAInd" as jsa_ind,
    "ESAInd" as esa_ind,
    "UCInd" as uc_ind,
    "PIPInd" as pip_ind,
    "OtherBenefitInd" as other_benefit_ind,
    "EmpSupportInd" as emp_support_ind,
    "EmpSupportReferral" as emp_support_referral,
    "EmpSupportDischargeDate" as emp_support_discharge_date,
    "EFFECTIVE_FROM" as effective_from,
    "RecordNumber" as record_number,
    "UniqueID_IDS004" as unique_id_ids004,
    "OrgID_Provider" as org_id_provider,
    "Person_ID" as person_id,
    "UniqueSubmissionID" as unique_submission_id,
    "Unique_MonthID" as unique_month_id,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicDateAdded" as dmic_date_added,
    "Unique_LocalPatientId" as unique_local_patient_id
from {{ source('iapt', 'IDS004EmpStatus') }}
