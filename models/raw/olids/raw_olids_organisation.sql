{{
    config(
        description="Raw layer (OLIDS stable layer - cleaned and filtered patient records). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.OLIDS.ORGANISATION \ndbt: source(''olids'', ''ORGANISATION'') \nColumns:\n  ID -> id\n  LDS_SOURCE_RECORD_ID -> lds_source_record_id\n  ORGANISATION_CODE -> organisation_code\n  ASSIGNING_AUTHORITY_CODE -> assigning_authority_code\n  NAME -> name\n  TYPE_DESCRIPTION -> type_description\n  PRIMARY_LOCATION_TYPE_SOURCE_CONCEPT_ID -> primary_location_type_source_concept_id\n  POSTCODE -> postcode\n  PARENT_ORGANISATION_ID -> parent_organisation_id\n  OPEN_DATE -> open_date\n  CLOSE_DATE -> close_date\n  IS_OBSOLETE -> is_obsolete\n  LDS_IS_DELETED -> lds_is_deleted\n  SOURCE_EXTRACTION_DATE -> source_extraction_date\n  LDS_TRANSFORM_DATETIME -> lds_transform_datetime"
    )
}}
select
    "ID" as id,
    "LDS_SOURCE_RECORD_ID" as lds_source_record_id,
    "ORGANISATION_CODE" as organisation_code,
    "ASSIGNING_AUTHORITY_CODE" as assigning_authority_code,
    "NAME" as name,
    "TYPE_DESCRIPTION" as type_description,
    "PRIMARY_LOCATION_TYPE_SOURCE_CONCEPT_ID" as primary_location_type_source_concept_id,
    "POSTCODE" as postcode,
    "PARENT_ORGANISATION_ID" as parent_organisation_id,
    "OPEN_DATE" as open_date,
    "CLOSE_DATE" as close_date,
    "IS_OBSOLETE" as is_obsolete,
    "LDS_IS_DELETED" as lds_is_deleted,
    "SOURCE_EXTRACTION_DATE" as source_extraction_date,
    "LDS_TRANSFORM_DATETIME" as lds_transform_datetime
from {{ source('olids', 'ORGANISATION') }}
