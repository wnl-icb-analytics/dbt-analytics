{{ config(materialized='view') }}

select
    appointment_id,
    person_id,
    sk_patient_id,
    patient_id,
    booked_at,
    scheduled_at,
    source_booking_method_code,
    source_booking_method_name,
    booking_method_code,
    booking_method_name,
    provider_organisation_id,
    provider_organisation_code,
    provider_organisation_code_authority,
    provider_organisation_name,
    publisher_organisation_id,
    publisher_organisation_code,
    publisher_organisation_name
from {{ ref('fct_gp_appointment') }}
where booked_at is not null
