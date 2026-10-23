{{
    config(
        description="Raw layer (ServicesDataLocal canonical-named feeds, WNL footprint). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SDL.RAID \ndbt: source(''sdl_wnl'', ''RAID'') \nColumns:\n  META_SK_ROW_ID -> meta_sk_row_id\n  META_FILE_ID -> meta_file_id\n  META_ROW_ID -> meta_row_id\n  META_BATCH_ID -> meta_batch_id\n  META_PARTITION_DATE -> meta_partition_date\n  META_PROVIDER_CODE -> meta_provider_code\n  META_RECIPIENT_CODE -> meta_recipient_code\n  META_VERSION_ID -> meta_version_id\n  ATTENDANCE_DATE -> attendance_date\n  CCG_CODE -> ccg_code\n  DEPARTMENT_ASSESSED -> department_assessed\n  DLP_BASELINE_FINANCIAL_MONTH -> dlp_baseline_financial_month\n  DLP_COMMISSIONER_CODE -> dlp_commissioner_code\n  DLP_FINANCIAL_MONTH -> dlp_financial_month\n  DLP_FINANCIAL_YEAR -> dlp_financial_year\n  DLP_FLEXOR_FREEZE -> dlp_flexor_freeze\n  DV_RECIPIENT_CODE -> dv_recipient_code\n  END_DATE -> end_date\n  HOSPITAL_NUMBER -> hospital_number\n  MONTH -> month\n  REFERRAL_DATE -> referral_date\n  REFERRED_TO_DRUGS_AND_ALCOHOL_SERVICES -> referred_to_drugs_and_alcohol_services\n  REFERRED_TO_MH_SERVICES -> referred_to_mh_services\n  SITE_CODE -> site_code\n  SITE_NAME -> site_name\n  SK_PATIENT_ID_NHS_NUMBER -> sk_patient_id_nhs_number\n  START_DATE -> start_date\n  TRUST_CODE -> trust_code\n  YEAR -> year"
    )
}}
select
    "META_SK_ROW_ID" as meta_sk_row_id,
    "META_FILE_ID" as meta_file_id,
    "META_ROW_ID" as meta_row_id,
    "META_BATCH_ID" as meta_batch_id,
    "META_PARTITION_DATE" as meta_partition_date,
    "META_PROVIDER_CODE" as meta_provider_code,
    "META_RECIPIENT_CODE" as meta_recipient_code,
    "META_VERSION_ID" as meta_version_id,
    "ATTENDANCE_DATE" as attendance_date,
    "CCG_CODE" as ccg_code,
    "DEPARTMENT_ASSESSED" as department_assessed,
    "DLP_BASELINE_FINANCIAL_MONTH" as dlp_baseline_financial_month,
    "DLP_COMMISSIONER_CODE" as dlp_commissioner_code,
    "DLP_FINANCIAL_MONTH" as dlp_financial_month,
    "DLP_FINANCIAL_YEAR" as dlp_financial_year,
    "DLP_FLEXOR_FREEZE" as dlp_flexor_freeze,
    "DV_RECIPIENT_CODE" as dv_recipient_code,
    "END_DATE" as end_date,
    "HOSPITAL_NUMBER" as hospital_number,
    "MONTH" as month,
    "REFERRAL_DATE" as referral_date,
    "REFERRED_TO_DRUGS_AND_ALCOHOL_SERVICES" as referred_to_drugs_and_alcohol_services,
    "REFERRED_TO_MH_SERVICES" as referred_to_mh_services,
    "SITE_CODE" as site_code,
    "SITE_NAME" as site_name,
    "SK_PATIENT_ID_NHS_NUMBER" as sk_patient_id_nhs_number,
    "START_DATE" as start_date,
    "TRUST_CODE" as trust_code,
    "YEAR" as year
from {{ source('sdl_wnl', 'RAID') }}
