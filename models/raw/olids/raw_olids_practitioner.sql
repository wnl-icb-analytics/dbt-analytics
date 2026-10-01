{{
    config(
        description="Raw layer (OLIDS stable layer - cleaned and filtered patient records). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.OLIDS.PRACTITIONER \ndbt: source(''olids'', ''PRACTITIONER'') \nColumns:\n  ID -> id\n  LDS_SOURCE_RECORD_ID -> lds_source_record_id\n  PUBLISHER_ORGANISATION_ID -> publisher_organisation_id\n  AUTHOR_ORGANISATION_ID -> author_organisation_id\n  GMC_CODE -> gmc_code\n  TITLE -> title\n  FIRST_NAME -> first_name\n  SURNAME -> surname\n  NAME -> name\n  IS_OBSOLETE -> is_obsolete\n  LDS_IS_DELETED -> lds_is_deleted\n  PUBLISHER_ORGANISATION_CODE -> publisher_organisation_code\n  SOURCE_EXTRACTION_DATE -> source_extraction_date\n  LDS_TRANSFORM_DATETIME -> lds_transform_datetime"
    )
}}
select
    "ID" as id,
    "LDS_SOURCE_RECORD_ID" as lds_source_record_id,
    "PUBLISHER_ORGANISATION_ID" as publisher_organisation_id,
    "AUTHOR_ORGANISATION_ID" as author_organisation_id,
    "GMC_CODE" as gmc_code,
    "TITLE" as title,
    "FIRST_NAME" as first_name,
    "SURNAME" as surname,
    "NAME" as name,
    "IS_OBSOLETE" as is_obsolete,
    "LDS_IS_DELETED" as lds_is_deleted,
    "PUBLISHER_ORGANISATION_CODE" as publisher_organisation_code,
    "SOURCE_EXTRACTION_DATE" as source_extraction_date,
    "LDS_TRANSFORM_DATETIME" as lds_transform_datetime
from {{ source('olids', 'PRACTITIONER') }}
