{{
    config(
        description="Raw layer (OLIDS stable layer - cleaned and filtered patient records). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.OLIDS.PATIENT_CONTACT \ndbt: source(''olids'', ''PATIENT_CONTACT'') \nColumns:\n  ID -> id\n  LDS_SOURCE_RECORD_ID -> lds_source_record_id\n  PATIENT_ID -> patient_id\n  PERSON_ID -> person_id\n  PUBLISHER_ORGANISATION_ID -> publisher_organisation_id\n  PROVIDER_ORGANISATION_ID -> provider_organisation_id\n  AUTHOR_ORGANISATION_ID -> author_organisation_id\n  CONTACT_TYPE_SOURCE_CONCEPT_ID -> contact_type_source_concept_id\n  START_DATE -> start_date\n  END_DATE -> end_date\n  LDS_IS_DELETED -> lds_is_deleted\n  PUBLISHER_ORGANISATION_CODE -> publisher_organisation_code\n  CLINICAL_SYSTEM -> clinical_system\n  SOURCE_EXTRACTION_DATE -> source_extraction_date\n  LDS_TRANSFORM_DATETIME -> lds_transform_datetime"
    )
}}
select
    "ID" as id,
    "LDS_SOURCE_RECORD_ID" as lds_source_record_id,
    "PATIENT_ID" as patient_id,
    "PERSON_ID" as person_id,
    "PUBLISHER_ORGANISATION_ID" as publisher_organisation_id,
    "PROVIDER_ORGANISATION_ID" as provider_organisation_id,
    "AUTHOR_ORGANISATION_ID" as author_organisation_id,
    "CONTACT_TYPE_SOURCE_CONCEPT_ID" as contact_type_source_concept_id,
    "START_DATE" as start_date,
    "END_DATE" as end_date,
    "LDS_IS_DELETED" as lds_is_deleted,
    "PUBLISHER_ORGANISATION_CODE" as publisher_organisation_code,
    "CLINICAL_SYSTEM" as clinical_system,
    "SOURCE_EXTRACTION_DATE" as source_extraction_date,
    "LDS_TRANSFORM_DATETIME" as lds_transform_datetime
from {{ source('olids', 'PATIENT_CONTACT') }}
