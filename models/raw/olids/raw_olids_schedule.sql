{{
    config(
        description="Raw layer (OLIDS stable layer - cleaned and filtered patient records). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.OLIDS.SCHEDULE \ndbt: source(''olids'', ''SCHEDULE'') \nColumns:\n  ID -> id\n  LDS_SOURCE_RECORD_ID -> lds_source_record_id\n  PUBLISHER_ORGANISATION_ID -> publisher_organisation_id\n  PROVIDER_ORGANISATION_ID -> provider_organisation_id\n  AUTHOR_ORGANISATION_ID -> author_organisation_id\n  LOCATION_ID -> location_id\n  LOCATION_NAME -> location_name\n  PRACTITIONER_ID -> practitioner_id\n  START_DATETIME -> start_datetime\n  END_DATETIME -> end_datetime\n  TYPE -> type\n  NAME -> name\n  IS_PRIVATE -> is_private\n  LDS_IS_DELETED -> lds_is_deleted\n  PUBLISHER_ORGANISATION_CODE -> publisher_organisation_code\n  SOURCE_EXTRACTION_DATE -> source_extraction_date\n  LDS_TRANSFORM_DATETIME -> lds_transform_datetime"
    )
}}
select
    "ID" as id,
    "LDS_SOURCE_RECORD_ID" as lds_source_record_id,
    "PUBLISHER_ORGANISATION_ID" as publisher_organisation_id,
    "PROVIDER_ORGANISATION_ID" as provider_organisation_id,
    "AUTHOR_ORGANISATION_ID" as author_organisation_id,
    "LOCATION_ID" as location_id,
    "LOCATION_NAME" as location_name,
    "PRACTITIONER_ID" as practitioner_id,
    "START_DATETIME" as start_datetime,
    "END_DATETIME" as end_datetime,
    "TYPE" as type,
    "NAME" as name,
    "IS_PRIVATE" as is_private,
    "LDS_IS_DELETED" as lds_is_deleted,
    "PUBLISHER_ORGANISATION_CODE" as publisher_organisation_code,
    "SOURCE_EXTRACTION_DATE" as source_extraction_date,
    "LDS_TRANSFORM_DATETIME" as lds_transform_datetime
from {{ source('olids', 'SCHEDULE') }}
