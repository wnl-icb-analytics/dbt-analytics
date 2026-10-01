{{
    config(
        description="Raw layer (OLIDS stable layer - cleaned and filtered patient records). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.OLIDS.EPISODE_OF_CARE_V2 \ndbt: source(''olids'', ''EPISODE_OF_CARE_V2'') \nColumns:\n  ID -> id\n  LDS_SOURCE_RECORD_ID -> lds_source_record_id\n  PATIENT_ID -> patient_id\n  PERSON_ID -> person_id\n  PUBLISHER_ORGANISATION_ID -> publisher_organisation_id\n  MANAGING_ORGANISATION_ID -> managing_organisation_id\n  AUTHOR_ORGANISATION_ID -> author_organisation_id\n  MANAGING_ORGANISATION_CODE -> managing_organisation_code\n  USUAL_PRACTITIONER_IN_ROLE_ID -> usual_practitioner_in_role_id\n  EPISODE_OF_CARE_START_DATE -> episode_of_care_start_date\n  EPISODE_OF_CARE_END_DATE -> episode_of_care_end_date\n  TYPE -> type\n  EPISODE_TYPE_SOURCE_CONCEPT_ID -> episode_type_source_concept_id\n  EPISODE_TYPE_SOURCE_CODE -> episode_type_source_code\n  EPISODE_TYPE_SOURCE_DISPLAY -> episode_type_source_display\n  EPISODE_TYPE_CODE -> episode_type_code\n  EPISODE_TYPE_DISPLAY -> episode_type_display\n  STATUS -> status\n  EPISODE_STATUS_SOURCE_CONCEPT_ID -> episode_status_source_concept_id\n  EPISODE_STATUS_SOURCE_CODE -> episode_status_source_code\n  EPISODE_STATUS_SOURCE_DISPLAY -> episode_status_source_display\n  EPISODE_STATUS_CODE -> episode_status_code\n  EPISODE_STATUS_DISPLAY -> episode_status_display\n  LDS_IS_DELETED -> lds_is_deleted\n  PUBLISHER_ORGANISATION_CODE -> publisher_organisation_code\n  CLINICAL_SYSTEM -> clinical_system\n  SOURCE_EXTRACTION_DATE -> source_extraction_date\n  LDS_TRANSFORM_DATETIME -> lds_transform_datetime"
    )
}}
select
    "ID" as id,
    "LDS_SOURCE_RECORD_ID" as lds_source_record_id,
    "PATIENT_ID" as patient_id,
    "PERSON_ID" as person_id,
    "PUBLISHER_ORGANISATION_ID" as publisher_organisation_id,
    "MANAGING_ORGANISATION_ID" as managing_organisation_id,
    "AUTHOR_ORGANISATION_ID" as author_organisation_id,
    "MANAGING_ORGANISATION_CODE" as managing_organisation_code,
    "USUAL_PRACTITIONER_IN_ROLE_ID" as usual_practitioner_in_role_id,
    "EPISODE_OF_CARE_START_DATE" as episode_of_care_start_date,
    "EPISODE_OF_CARE_END_DATE" as episode_of_care_end_date,
    "TYPE" as type,
    "EPISODE_TYPE_SOURCE_CONCEPT_ID" as episode_type_source_concept_id,
    "EPISODE_TYPE_SOURCE_CODE" as episode_type_source_code,
    "EPISODE_TYPE_SOURCE_DISPLAY" as episode_type_source_display,
    "EPISODE_TYPE_CODE" as episode_type_code,
    "EPISODE_TYPE_DISPLAY" as episode_type_display,
    "STATUS" as status,
    "EPISODE_STATUS_SOURCE_CONCEPT_ID" as episode_status_source_concept_id,
    "EPISODE_STATUS_SOURCE_CODE" as episode_status_source_code,
    "EPISODE_STATUS_SOURCE_DISPLAY" as episode_status_source_display,
    "EPISODE_STATUS_CODE" as episode_status_code,
    "EPISODE_STATUS_DISPLAY" as episode_status_display,
    "LDS_IS_DELETED" as lds_is_deleted,
    "PUBLISHER_ORGANISATION_CODE" as publisher_organisation_code,
    "CLINICAL_SYSTEM" as clinical_system,
    "SOURCE_EXTRACTION_DATE" as source_extraction_date,
    "LDS_TRANSFORM_DATETIME" as lds_transform_datetime
from {{ source('olids', 'EPISODE_OF_CARE_V2') }}
