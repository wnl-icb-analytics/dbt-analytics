{{
    config(
        description="Raw layer (OLIDS stable layer - cleaned and filtered patient records). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.OLIDS.PATIENT \ndbt: source(''olids'', ''PATIENT'') \nColumns:\n  ID -> id\n  LDS_SOURCE_RECORD_ID -> lds_source_record_id\n  PERSON_ID -> person_id\n  PUBLISHER_ORGANISATION_ID -> publisher_organisation_id\n  PROVIDER_ORGANISATION_ID -> provider_organisation_id\n  AUTHOR_ORGANISATION_ID -> author_organisation_id\n  REGISTERED_PRACTICE_ORGANISATION_ID -> registered_practice_organisation_id\n  LOCAL_PATIENT_ID -> local_patient_id\n  SK_PATIENT_ID -> sk_patient_id\n  PDS_TRACE_STATUS -> pds_trace_status\n  SK_LINKAGE_STATUS -> sk_linkage_status\n  TITLE -> title\n  GENDER_SOURCE_CONCEPT_ID -> gender_source_concept_id\n  GENDER_SOURCE_CODE -> gender_source_code\n  GENDER_SOURCE_DISPLAY -> gender_source_display\n  GENDER_CODE -> gender_code\n  GENDER_DISPLAY -> gender_display\n  BIRTH_DATE -> birth_date\n  BIRTH_YEAR -> birth_year\n  BIRTH_MONTH -> birth_month\n  BIRTH_WEEK_ISO -> birth_week_iso\n  BIRTH_DAY -> birth_day\n  DEATH_DATE -> death_date\n  DEATH_YEAR -> death_year\n  DEATH_MONTH -> death_month\n  DEATH_WEEK_ISO -> death_week_iso\n  IS_CONFIDENTIAL -> is_confidential\n  IS_TEST_PATIENT -> is_test_patient\n  IS_SPINE_SENSITIVE -> is_spine_sensitive\n  LDS_SOURCE_DATASET -> lds_source_dataset\n  LDS_IS_DELETED -> lds_is_deleted\n  PUBLISHER_ORGANISATION_CODE -> publisher_organisation_code\n  CLINICAL_SYSTEM -> clinical_system\n  SOURCE_EXTRACTION_DATE -> source_extraction_date\n  LDS_TRANSFORM_DATETIME -> lds_transform_datetime"
    )
}}
select
    "ID" as id,
    "LDS_SOURCE_RECORD_ID" as lds_source_record_id,
    "PERSON_ID" as person_id,
    "PUBLISHER_ORGANISATION_ID" as publisher_organisation_id,
    "PROVIDER_ORGANISATION_ID" as provider_organisation_id,
    "AUTHOR_ORGANISATION_ID" as author_organisation_id,
    "REGISTERED_PRACTICE_ORGANISATION_ID" as registered_practice_organisation_id,
    "LOCAL_PATIENT_ID" as local_patient_id,
    "SK_PATIENT_ID" as sk_patient_id,
    "PDS_TRACE_STATUS" as pds_trace_status,
    "SK_LINKAGE_STATUS" as sk_linkage_status,
    "TITLE" as title,
    "GENDER_SOURCE_CONCEPT_ID" as gender_source_concept_id,
    "GENDER_SOURCE_CODE" as gender_source_code,
    "GENDER_SOURCE_DISPLAY" as gender_source_display,
    "GENDER_CODE" as gender_code,
    "GENDER_DISPLAY" as gender_display,
    "BIRTH_DATE" as birth_date,
    "BIRTH_YEAR" as birth_year,
    "BIRTH_MONTH" as birth_month,
    "BIRTH_WEEK_ISO" as birth_week_iso,
    "BIRTH_DAY" as birth_day,
    "DEATH_DATE" as death_date,
    "DEATH_YEAR" as death_year,
    "DEATH_MONTH" as death_month,
    "DEATH_WEEK_ISO" as death_week_iso,
    "IS_CONFIDENTIAL" as is_confidential,
    "IS_TEST_PATIENT" as is_test_patient,
    "IS_SPINE_SENSITIVE" as is_spine_sensitive,
    "LDS_SOURCE_DATASET" as lds_source_dataset,
    "LDS_IS_DELETED" as lds_is_deleted,
    "PUBLISHER_ORGANISATION_CODE" as publisher_organisation_code,
    "CLINICAL_SYSTEM" as clinical_system,
    "SOURCE_EXTRACTION_DATE" as source_extraction_date,
    "LDS_TRANSFORM_DATETIME" as lds_transform_datetime
from {{ source('olids', 'PATIENT') }}
