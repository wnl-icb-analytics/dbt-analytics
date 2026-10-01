{{
    config(
        description="Raw layer (OLIDS stable layer - cleaned and filtered patient records). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.OLIDS.SCHEDULE_PRACTITIONER \ndbt: source(''olids'', ''SCHEDULE_PRACTITIONER'') \nColumns:\n  ID -> id\n  LDS_SOURCE_RECORD_ID -> lds_source_record_id\n  PUBLISHER_ORGANISATION_ID -> publisher_organisation_id\n  PROVIDER_ORGANISATION_ID -> provider_organisation_id\n  AUTHOR_ORGANISATION_ID -> author_organisation_id\n  SCHEDULE_ID -> schedule_id\n  PRACTITIONER_ID -> practitioner_id\n  LDS_IS_DELETED -> lds_is_deleted\n  PUBLISHER_ORGANISATION_CODE -> publisher_organisation_code\n  SOURCE_EXTRACTION_DATE -> source_extraction_date\n  LDS_TRANSFORM_DATETIME -> lds_transform_datetime"
    )
}}
select
    "ID" as id,
    "LDS_SOURCE_RECORD_ID" as lds_source_record_id,
    "PUBLISHER_ORGANISATION_ID" as publisher_organisation_id,
    "PROVIDER_ORGANISATION_ID" as provider_organisation_id,
    "AUTHOR_ORGANISATION_ID" as author_organisation_id,
    "SCHEDULE_ID" as schedule_id,
    "PRACTITIONER_ID" as practitioner_id,
    "LDS_IS_DELETED" as lds_is_deleted,
    "PUBLISHER_ORGANISATION_CODE" as publisher_organisation_code,
    "SOURCE_EXTRACTION_DATE" as source_extraction_date,
    "LDS_TRANSFORM_DATETIME" as lds_transform_datetime
from {{ source('olids', 'SCHEDULE_PRACTITIONER') }}
