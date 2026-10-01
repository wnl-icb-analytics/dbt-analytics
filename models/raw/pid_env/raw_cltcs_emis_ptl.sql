{{
    config(
        description="Raw layer (EMIS extract for c-ltcs pipeline from PID environment via MESH and other local lookups). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.CLTCS.PTL \ndbt: source(''cltcs_emis_extract'', ''PTL'') \nColumns:\n  SK_PATIENT_ID -> sk_patient_id\n  PCN_CODE -> pcn_code\n  RECORD_SOURCE -> record_source\n  DEMOGRAPHICS_DATE_OF_MDT -> demographics_date_of_mdt\n  SUMMARIES_DESKTOP_REVIEW_OUTCOME -> summaries_desktop_review_outcome\n  MDT_IMPACT_SCORE -> mdt_impact_score\n  MDT_CLINICIAN_VALUE -> mdt_clinician_value\n  MDT_SUITABLE_CASE_STUDY -> mdt_suitable_case_study\n  METADATA_FILE_PATH -> metadata_file_path\n  METADATA_FILE_ROW_NUMBER -> metadata_file_row_number\n  METADATA_RECORD_INGESTION_TIMESTAMP -> metadata_record_ingestion_timestamp\n  METADATA_FILE_CONTENT_KEY -> metadata_file_content_key\n  METADATA_FILE_LAST_MODIFIED -> metadata_file_last_modified"
    )
}}
select
    "SK_PATIENT_ID" as sk_patient_id,
    "PCN_CODE" as pcn_code,
    "RECORD_SOURCE" as record_source,
    "DEMOGRAPHICS_DATE_OF_MDT" as demographics_date_of_mdt,
    "SUMMARIES_DESKTOP_REVIEW_OUTCOME" as summaries_desktop_review_outcome,
    "MDT_IMPACT_SCORE" as mdt_impact_score,
    "MDT_CLINICIAN_VALUE" as mdt_clinician_value,
    "MDT_SUITABLE_CASE_STUDY" as mdt_suitable_case_study,
    "METADATA_FILE_PATH" as metadata_file_path,
    "METADATA_FILE_ROW_NUMBER" as metadata_file_row_number,
    "METADATA_RECORD_INGESTION_TIMESTAMP" as metadata_record_ingestion_timestamp,
    "METADATA_FILE_CONTENT_KEY" as metadata_file_content_key,
    "METADATA_FILE_LAST_MODIFIED" as metadata_file_last_modified
from {{ source('cltcs_emis_extract', 'PTL') }}
