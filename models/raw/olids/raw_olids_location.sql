{{
    config(
        description="Raw layer (OLIDS stable layer - cleaned and filtered patient records). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.OLIDS.LOCATION \ndbt: source(''olids'', ''LOCATION'') \nColumns:\n  ID -> id\n  LDS_SOURCE_RECORD_ID -> lds_source_record_id\n  NAME -> name\n  LOCATION_TYPE_SOURCE_CONCEPT_ID -> location_type_source_concept_id\n  TYPE_DESCRIPTION -> type_description\n  IS_PRIMARY_LOCATION -> is_primary_location\n  HOUSE_NAME -> house_name\n  HOUSE_NUMBER -> house_number\n  HOUSE_NAME_FLAT_NUMBER -> house_name_flat_number\n  STREET -> street\n  ADDRESS_LINE_1 -> address_line_1\n  ADDRESS_LINE_2 -> address_line_2\n  ADDRESS_LINE_3 -> address_line_3\n  ADDRESS_LINE_4 -> address_line_4\n  POSTCODE -> postcode\n  MANAGING_ORGANISATION_ID -> managing_organisation_id\n  OPEN_DATE -> open_date\n  CLOSE_DATE -> close_date\n  IS_OBSOLETE -> is_obsolete\n  LDS_IS_DELETED -> lds_is_deleted\n  SOURCE_EXTRACTION_DATE -> source_extraction_date\n  LDS_TRANSFORM_DATETIME -> lds_transform_datetime"
    )
}}
select
    "ID" as id,
    "LDS_SOURCE_RECORD_ID" as lds_source_record_id,
    "NAME" as name,
    "LOCATION_TYPE_SOURCE_CONCEPT_ID" as location_type_source_concept_id,
    "TYPE_DESCRIPTION" as type_description,
    "IS_PRIMARY_LOCATION" as is_primary_location,
    "HOUSE_NAME" as house_name,
    "HOUSE_NUMBER" as house_number,
    "HOUSE_NAME_FLAT_NUMBER" as house_name_flat_number,
    "STREET" as street,
    "ADDRESS_LINE_1" as address_line_1,
    "ADDRESS_LINE_2" as address_line_2,
    "ADDRESS_LINE_3" as address_line_3,
    "ADDRESS_LINE_4" as address_line_4,
    "POSTCODE" as postcode,
    "MANAGING_ORGANISATION_ID" as managing_organisation_id,
    "OPEN_DATE" as open_date,
    "CLOSE_DATE" as close_date,
    "IS_OBSOLETE" as is_obsolete,
    "LDS_IS_DELETED" as lds_is_deleted,
    "SOURCE_EXTRACTION_DATE" as source_extraction_date,
    "LDS_TRANSFORM_DATETIME" as lds_transform_datetime
from {{ source('olids', 'LOCATION') }}
