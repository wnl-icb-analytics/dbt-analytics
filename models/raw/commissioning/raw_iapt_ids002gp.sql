{{
    config(
        description="Raw layer (NHS Talking Therapies (IAPT) dataset). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.IAPT.IDS002GP \ndbt: source(''iapt'', ''IDS002GP'') \nColumns:\n  SK -> sk\n  LocalPatientId -> local_patient_id\n  GMPCodeReg -> gmp_code_reg\n  StartDateGMPRegistration -> start_date_gmp_registration\n  EndDateGMPRegistration -> end_date_gmp_registration\n  OrgIDGPPrac -> org_idgp_prac\n  EFFECTIVE_FROM -> effective_from\n  RecordNumber -> record_number\n  UniqueID_IDS002 -> unique_id_ids002\n  OrgID_Provider -> org_id_provider\n  Person_ID -> person_id\n  UniqueSubmissionID -> unique_submission_id\n  OrgID_CCG_GP -> org_id_ccg_gp\n  OrgIDSubICBLocGP -> org_id_sub_icb_loc_gp\n  OrgIDICBGPPractice -> org_idicbgp_practice\n  OrgIDSubICBREGP -> org_id_sub_icbregp\n  DistanceFromHome_GP -> distance_from_home_gp\n  Unique_MonthID -> unique_month_id\n  RecordStartDate -> record_start_date\n  RecordEndDate -> record_end_date\n  LADistrictAuthGPPractice -> la_district_auth_gp_practice\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicDateAdded -> dmic_date_added\n  Unique_LocalPatientId -> unique_local_patient_id\n  dmIcbRegistrationSubmitted -> dm_icb_registration_submitted\n  dmSubIcbRegistrationSubmitted -> dm_sub_icb_registration_submitted\n  dmCommissionerDerivationReason -> dm_commissioner_derivation_reason"
    )
}}
select
    "SK" as sk,
    "LocalPatientId" as local_patient_id,
    "GMPCodeReg" as gmp_code_reg,
    "StartDateGMPRegistration" as start_date_gmp_registration,
    "EndDateGMPRegistration" as end_date_gmp_registration,
    "OrgIDGPPrac" as org_idgp_prac,
    "EFFECTIVE_FROM" as effective_from,
    "RecordNumber" as record_number,
    "UniqueID_IDS002" as unique_id_ids002,
    "OrgID_Provider" as org_id_provider,
    "Person_ID" as person_id,
    "UniqueSubmissionID" as unique_submission_id,
    "OrgID_CCG_GP" as org_id_ccg_gp,
    "OrgIDSubICBLocGP" as org_id_sub_icb_loc_gp,
    "OrgIDICBGPPractice" as org_idicbgp_practice,
    "OrgIDSubICBREGP" as org_id_sub_icbregp,
    "DistanceFromHome_GP" as distance_from_home_gp,
    "Unique_MonthID" as unique_month_id,
    "RecordStartDate" as record_start_date,
    "RecordEndDate" as record_end_date,
    "LADistrictAuthGPPractice" as la_district_auth_gp_practice,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicDateAdded" as dmic_date_added,
    "Unique_LocalPatientId" as unique_local_patient_id,
    "dmIcbRegistrationSubmitted" as dm_icb_registration_submitted,
    "dmSubIcbRegistrationSubmitted" as dm_sub_icb_registration_submitted,
    "dmCommissionerDerivationReason" as dm_commissioner_derivation_reason
from {{ source('iapt', 'IDS002GP') }}
