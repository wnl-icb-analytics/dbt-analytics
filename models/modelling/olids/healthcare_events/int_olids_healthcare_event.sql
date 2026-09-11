{{ config(materialized='view', tags=['healthcare_event_stream']) }}

-- The person-clustered rows are materialised once in dbt-OLIDS.
select
    'OLIDS:' || healthcare_event_id::varchar as event_id,
    sk_patient_id::varchar as sk_patient_id,
    person_id::varchar as source_person_id,
    'OLIDS'::varchar as source_dataset,
    source_record_type::varchar as source_record_type,
    source_record_id::varchar as source_record_id,
    case source_record_type
        when 'appointment' then 'fct_gp_appointment'
        when 'referral_request' then 'fct_gp_referral_request'
    end as source_model_name,
    event_date,
    event_at,
    case
        when event_date is null then 'unknown'
        when event_time_precision = 'day' then 'date'
        else event_time_precision
    end as event_time_precision,
    event_time_basis,
    event_type,
    case event_type
        when 'appointment_booking' then 'Appointment booked'
        when 'appointment_slot' then 'Scheduled appointment'
        when 'patient_referral' then 'Patient referral'
    end as event_name,
    'primary_care'::varchar as care_setting,
    status_code,
    status_name,
    event_code,
    event_code_name,
    event_coding_system,
    'OLIDS:' || clinical_record_id::varchar as clinical_record_id,
    provider_organisation_code,
    provider_organisation_name,
    provider_code_authority,
    source_extraction_date as source_received_at
from {{ ref('fct_gp_healthcare_event') }}
