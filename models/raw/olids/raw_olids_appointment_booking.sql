{{
    config(
        description="Raw layer (OLIDS stable layer - cleaned and filtered patient records). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.OLIDS.APPOINTMENT_BOOKING \ndbt: source(''olids'', ''APPOINTMENT_BOOKING'') \nColumns:\n  APPOINTMENT_ID -> appointment_id\n  PATIENT_ID -> patient_id\n  PERSON_ID -> person_id\n  DATETIME_BOOKED -> datetime_booked\n  START_DATE -> start_date\n  BOOKING_METHOD_SOURCE_CODE -> booking_method_source_code\n  BOOKING_METHOD_SOURCE_DISPLAY -> booking_method_source_display\n  BOOKING_METHOD_CODE -> booking_method_code\n  BOOKING_METHOD_DISPLAY -> booking_method_display\n  PROVIDER_ORGANISATION_ID -> provider_organisation_id\n  PUBLISHER_ORGANISATION_ID -> publisher_organisation_id\n  PUBLISHER_ORGANISATION_CODE -> publisher_organisation_code\n  SOURCE_EXTRACTION_DATE -> source_extraction_date"
    )
}}
select
    "APPOINTMENT_ID" as appointment_id,
    "PATIENT_ID" as patient_id,
    "PERSON_ID" as person_id,
    "DATETIME_BOOKED" as datetime_booked,
    "START_DATE" as start_date,
    "BOOKING_METHOD_SOURCE_CODE" as booking_method_source_code,
    "BOOKING_METHOD_SOURCE_DISPLAY" as booking_method_source_display,
    "BOOKING_METHOD_CODE" as booking_method_code,
    "BOOKING_METHOD_DISPLAY" as booking_method_display,
    "PROVIDER_ORGANISATION_ID" as provider_organisation_id,
    "PUBLISHER_ORGANISATION_ID" as publisher_organisation_id,
    "PUBLISHER_ORGANISATION_CODE" as publisher_organisation_code,
    "SOURCE_EXTRACTION_DATE" as source_extraction_date
from {{ source('olids', 'APPOINTMENT_BOOKING') }}
