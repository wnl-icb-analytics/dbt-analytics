{{
    config(
        description="Raw layer (OLIDS stable layer - cleaned and filtered patient records). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.OLIDS.HEALTHCARE_EVENT \ndbt: source(''olids'', ''HEALTHCARE_EVENT'') \nColumns:\n  HEALTHCARE_EVENT_ID -> healthcare_event_id\n  EVENT_TYPE -> event_type\n  PERSON_ID -> person_id\n  PATIENT_ID -> patient_id\n  SK_PATIENT_ID -> sk_patient_id\n  EVENT_AT -> event_at\n  EVENT_DATE -> event_date\n  EVENT_TIME_PRECISION -> event_time_precision\n  EVENT_TIME_BASIS -> event_time_basis\n  SOURCE_RECORD_TYPE -> source_record_type\n  SOURCE_RECORD_ID -> source_record_id\n  APPOINTMENT_ID -> appointment_id\n  CLINICAL_RECORD_ID -> clinical_record_id\n  EVENT_CODE -> event_code\n  EVENT_CODE_NAME -> event_code_name\n  EVENT_CODING_SYSTEM -> event_coding_system\n  SOURCE_STATUS_CODE -> source_status_code\n  SOURCE_STATUS_NAME -> source_status_name\n  STATUS_CODE -> status_code\n  STATUS_NAME -> status_name\n  PROVIDER_ORGANISATION_ID -> provider_organisation_id\n  PUBLISHER_ORGANISATION_ID -> publisher_organisation_id\n  PUBLISHER_ORGANISATION_CODE -> publisher_organisation_code\n  SOURCE_EXTRACTION_DATE -> source_extraction_date\n  PROVIDER_ORGANISATION_CODE -> provider_organisation_code\n  PROVIDER_CODE_AUTHORITY -> provider_code_authority\n  PROVIDER_ORGANISATION_NAME -> provider_organisation_name\n  PUBLISHER_ORGANISATION_NAME -> publisher_organisation_name"
    )
}}
select
    "HEALTHCARE_EVENT_ID" as healthcare_event_id,
    "EVENT_TYPE" as event_type,
    "PERSON_ID" as person_id,
    "PATIENT_ID" as patient_id,
    "SK_PATIENT_ID" as sk_patient_id,
    "EVENT_AT" as event_at,
    "EVENT_DATE" as event_date,
    "EVENT_TIME_PRECISION" as event_time_precision,
    "EVENT_TIME_BASIS" as event_time_basis,
    "SOURCE_RECORD_TYPE" as source_record_type,
    "SOURCE_RECORD_ID" as source_record_id,
    "APPOINTMENT_ID" as appointment_id,
    "CLINICAL_RECORD_ID" as clinical_record_id,
    "EVENT_CODE" as event_code,
    "EVENT_CODE_NAME" as event_code_name,
    "EVENT_CODING_SYSTEM" as event_coding_system,
    "SOURCE_STATUS_CODE" as source_status_code,
    "SOURCE_STATUS_NAME" as source_status_name,
    "STATUS_CODE" as status_code,
    "STATUS_NAME" as status_name,
    "PROVIDER_ORGANISATION_ID" as provider_organisation_id,
    "PUBLISHER_ORGANISATION_ID" as publisher_organisation_id,
    "PUBLISHER_ORGANISATION_CODE" as publisher_organisation_code,
    "SOURCE_EXTRACTION_DATE" as source_extraction_date,
    "PROVIDER_ORGANISATION_CODE" as provider_organisation_code,
    "PROVIDER_CODE_AUTHORITY" as provider_code_authority,
    "PROVIDER_ORGANISATION_NAME" as provider_organisation_name,
    "PUBLISHER_ORGANISATION_NAME" as publisher_organisation_name
from {{ source('olids', 'HEALTHCARE_EVENT') }}
