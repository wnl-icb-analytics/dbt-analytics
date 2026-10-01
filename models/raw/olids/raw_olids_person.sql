{{
    config(
        description="Raw layer (OLIDS stable layer - cleaned and filtered patient records). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.OLIDS.PERSON \ndbt: source(''olids'', ''PERSON'') \nColumns:\n  ID -> id\n  PERSON_UUID -> person_uuid\n  LDS_SOURCE_RECORD_ID -> lds_source_record_id\n  REQ_NHS_NUMBER -> req_nhs_number\n  MATCHED_NHS_NO -> matched_nhs_no\n  GENDER -> gender\n  DATE_OF_BIRTH -> date_of_birth\n  DATE_OF_BIRTH_YEAR -> date_of_birth_year\n  DATE_OF_BIRTH_MONTH -> date_of_birth_month\n  DATE_OF_BIRTH_PRECISION -> date_of_birth_precision\n  DATE_OF_DEATH -> date_of_death\n  DATE_OF_DEATH_YEAR -> date_of_death_year\n  DATE_OF_DEATH_MONTH -> date_of_death_month\n  DATE_OF_DEATH_PRECISION -> date_of_death_precision\n  DEATH_NOTIFICATION_STATUS -> death_notification_status\n  POSTCODE -> postcode\n  PREFERRED_CONTACT_METHOD -> preferred_contact_method\n  NOMINATED_PHARMACY -> nominated_pharmacy\n  DISPENSING_DOCTOR -> dispensing_doctor\n  MEDICAL_APPLIANCE_SUPPLIER -> medical_appliance_supplier\n  GP_PRACTICE_CODE -> gp_practice_code\n  GP_REGISTRATION_DATE -> gp_registration_date\n  NHAIS_POSTING_ID -> nhais_posting_id\n  IS_PATIENT_FLAGGED_SENSITIVE -> is_patient_flagged_sensitive\n  IS_LATEST_FOR_PERSON -> is_latest_for_person\n  ERROR_SUCCESS_CODE -> error_success_code\n  LDS_IS_DELETED -> lds_is_deleted\n  CLINICAL_SYSTEM -> clinical_system\n  SOURCE_EXTRACTION_DATE -> source_extraction_date\n  LDS_TRANSFORM_DATETIME -> lds_transform_datetime"
    )
}}
select
    "ID" as id,
    "PERSON_UUID" as person_uuid,
    "LDS_SOURCE_RECORD_ID" as lds_source_record_id,
    "REQ_NHS_NUMBER" as req_nhs_number,
    "MATCHED_NHS_NO" as matched_nhs_no,
    "GENDER" as gender,
    "DATE_OF_BIRTH" as date_of_birth,
    "DATE_OF_BIRTH_YEAR" as date_of_birth_year,
    "DATE_OF_BIRTH_MONTH" as date_of_birth_month,
    "DATE_OF_BIRTH_PRECISION" as date_of_birth_precision,
    "DATE_OF_DEATH" as date_of_death,
    "DATE_OF_DEATH_YEAR" as date_of_death_year,
    "DATE_OF_DEATH_MONTH" as date_of_death_month,
    "DATE_OF_DEATH_PRECISION" as date_of_death_precision,
    "DEATH_NOTIFICATION_STATUS" as death_notification_status,
    "POSTCODE" as postcode,
    "PREFERRED_CONTACT_METHOD" as preferred_contact_method,
    "NOMINATED_PHARMACY" as nominated_pharmacy,
    "DISPENSING_DOCTOR" as dispensing_doctor,
    "MEDICAL_APPLIANCE_SUPPLIER" as medical_appliance_supplier,
    "GP_PRACTICE_CODE" as gp_practice_code,
    "GP_REGISTRATION_DATE" as gp_registration_date,
    "NHAIS_POSTING_ID" as nhais_posting_id,
    "IS_PATIENT_FLAGGED_SENSITIVE" as is_patient_flagged_sensitive,
    "IS_LATEST_FOR_PERSON" as is_latest_for_person,
    "ERROR_SUCCESS_CODE" as error_success_code,
    "LDS_IS_DELETED" as lds_is_deleted,
    "CLINICAL_SYSTEM" as clinical_system,
    "SOURCE_EXTRACTION_DATE" as source_extraction_date,
    "LDS_TRANSFORM_DATETIME" as lds_transform_datetime
from {{ source('olids', 'PERSON') }}
