{{
    config(
        description="Raw layer (OLIDS stable layer - cleaned and filtered patient records). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.OLIDS.PATIENT_PERSON \ndbt: source(''olids'', ''PATIENT_PERSON'') \nColumns:\n  ID -> id\n  LDS_SOURCE_RECORD_ID -> lds_source_record_id\n  PATIENT_ID -> patient_id\n  PERSON_ID -> person_id\n  PERSON_UUID -> person_uuid\n  LDS_BUSINESS_ID_PERSON -> lds_business_id_person\n  LDS_SOURCE_RECORD_ID_PERSON -> lds_source_record_id_person\n  GP_PRACTICE_CODE -> gp_practice_code\n  LDS_IS_DELETED -> lds_is_deleted\n  LDS_TRANSFORM_DATETIME -> lds_transform_datetime\n  CLINICAL_SYSTEM -> clinical_system"
    )
}}
select
    "ID" as id,
    "LDS_SOURCE_RECORD_ID" as lds_source_record_id,
    "PATIENT_ID" as patient_id,
    "PERSON_ID" as person_id,
    "PERSON_UUID" as person_uuid,
    "LDS_BUSINESS_ID_PERSON" as lds_business_id_person,
    "LDS_SOURCE_RECORD_ID_PERSON" as lds_source_record_id_person,
    "GP_PRACTICE_CODE" as gp_practice_code,
    "LDS_IS_DELETED" as lds_is_deleted,
    "LDS_TRANSFORM_DATETIME" as lds_transform_datetime,
    "CLINICAL_SYSTEM" as clinical_system
from {{ source('olids', 'PATIENT_PERSON') }}
