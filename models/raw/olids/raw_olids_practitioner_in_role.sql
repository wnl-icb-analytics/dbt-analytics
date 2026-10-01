{{
    config(
        description="Raw layer (OLIDS stable layer - cleaned and filtered patient records). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.OLIDS.PRACTITIONER_IN_ROLE \ndbt: source(''olids'', ''PRACTITIONER_IN_ROLE'') \nColumns:\n  ID -> id\n  LDS_SOURCE_RECORD_ID -> lds_source_record_id\n  PUBLISHER_ORGANISATION_ID -> publisher_organisation_id\n  AUTHOR_ORGANISATION_ID -> author_organisation_id\n  PRACTITIONER_ID -> practitioner_id\n  EMPLOYER_ORGANISATION_ID -> employer_organisation_id\n  ROLE_CODE -> role_code\n  ROLE -> role\n  DATE_EMPLOYMENT_START -> date_employment_start\n  DATE_EMPLOYMENT_END -> date_employment_end\n  LDS_IS_DELETED -> lds_is_deleted\n  PUBLISHER_ORGANISATION_CODE -> publisher_organisation_code\n  SOURCE_EXTRACTION_DATE -> source_extraction_date\n  LDS_TRANSFORM_DATETIME -> lds_transform_datetime"
    )
}}
select
    "ID" as id,
    "LDS_SOURCE_RECORD_ID" as lds_source_record_id,
    "PUBLISHER_ORGANISATION_ID" as publisher_organisation_id,
    "AUTHOR_ORGANISATION_ID" as author_organisation_id,
    "PRACTITIONER_ID" as practitioner_id,
    "EMPLOYER_ORGANISATION_ID" as employer_organisation_id,
    "ROLE_CODE" as role_code,
    "ROLE" as role,
    "DATE_EMPLOYMENT_START" as date_employment_start,
    "DATE_EMPLOYMENT_END" as date_employment_end,
    "LDS_IS_DELETED" as lds_is_deleted,
    "PUBLISHER_ORGANISATION_CODE" as publisher_organisation_code,
    "SOURCE_EXTRACTION_DATE" as source_extraction_date,
    "LDS_TRANSFORM_DATETIME" as lds_transform_datetime
from {{ source('olids', 'PRACTITIONER_IN_ROLE') }}
