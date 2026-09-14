{{ config(materialized='view') }}

select
    b.appointment_id,
    b.person_id,
    p.sk_patient_id,
    b.patient_id,
    b.datetime_booked as booked_at,
    b.start_date as scheduled_at,
    b.booking_method_source_code as source_booking_method_code,
    b.booking_method_source_display as source_booking_method_name,
    b.booking_method_code,
    b.booking_method_display as booking_method_name,
    b.provider_organisation_id,
    provider.organisation_code as provider_organisation_code,
    provider.organisation_code_assigning_authority as provider_organisation_code_authority,
    provider.name as provider_organisation_name,
    b.publisher_organisation_id,
    b.publisher_organisation_code,
    publisher.name as publisher_organisation_name
from {{ ref('stg_olids_appointment_booking') }} as b
left join {{ ref('dim_person_pseudo') }} as p
    on b.person_id = p.person_id
left join {{ ref('stg_olids_organisation') }} as provider
    on b.provider_organisation_id = provider.id
left join {{ ref('stg_olids_organisation') }} as publisher
    on b.publisher_organisation_id = publisher.id
