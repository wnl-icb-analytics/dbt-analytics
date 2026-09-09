{{
    config(
        description="Raw layer: Current source-recorded appointment to clinical record relationships, prepared in dbt-OLIDS.. 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.OLIDS.APPOINTMENT_CLINICAL_RECORD \ndbt: source(''olids'', ''APPOINTMENT_CLINICAL_RECORD'') \nColumns:\n  APPOINTMENT_ID -> appointment_id\n  CLINICAL_RECORD_TYPE -> clinical_record_type\n  CLINICAL_RECORD_ID -> clinical_record_id\n  ENCOUNTER_ID -> encounter_id\n  PATIENT_ID -> patient_id\n  PERSON_ID -> person_id"
    )
}}
select
    "APPOINTMENT_ID" as appointment_id,
    "CLINICAL_RECORD_TYPE" as clinical_record_type,
    "CLINICAL_RECORD_ID" as clinical_record_id,
    "ENCOUNTER_ID" as encounter_id,
    "PATIENT_ID" as patient_id,
    "PERSON_ID" as person_id
from {{ source('olids', 'APPOINTMENT_CLINICAL_RECORD') }}
