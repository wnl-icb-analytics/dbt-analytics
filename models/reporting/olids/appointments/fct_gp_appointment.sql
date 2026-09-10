{{ config(materialized='view') }}

select
    a.id as appointment_id,
    a.person_id,
    p.sk_patient_id,
    a.patient_id,
    a.datetime_booked as booked_at,
    a.start_date as scheduled_at,
    a.datetime_sent_in as patient_sent_in_at,
    a.datetime_left as patient_left_at,
    a.planned_duration_mins as planned_duration_minutes,
    a.actual_duration_mins as actual_duration_minutes,
    a.patient_wait_mins as patient_wait_minutes,
    a.patient_delay_mins as patient_delay_minutes,
    a.appointment_status_source_code as source_status_code,
    a.appointment_status_source_display as source_status_name,
    a.appointment_status_code as status_code,
    a.appointment_status_display as status_name,
    a.booking_method_source_code as source_booking_method_code,
    a.booking_method_source_display as source_booking_method_name,
    a.booking_method_code,
    a.booking_method_display as booking_method_name,
    a.contact_mode_source_code as source_contact_mode_code,
    a.contact_mode_source_display as source_contact_mode_name,
    a.contact_mode_code,
    a.contact_mode_display as contact_mode_name,
    a.is_blocked,
    a.appointment_type as local_slot_type,
    a.national_slot_category_name,
    a.national_slot_category_description,
    a.context_type,
    a.service_setting,
    a.age_at_event,
    a.schedule_id,
    schedule.type as schedule_type,
    a.practitioner_in_role_id,
    practitioner.practitioner_id,
    practitioner.role_code as practitioner_role_code,
    practitioner.role as practitioner_role_name,
    a.provider_organisation_id,
    provider.organisation_code as provider_organisation_code,
    provider.organisation_code_assigning_authority as provider_organisation_code_authority,
    provider.name as provider_organisation_name,
    a.publisher_organisation_id,
    a.publisher_organisation_code,
    publisher.name as publisher_organisation_name
from {{ ref('stg_olids_appointment') }} as a
left join {{ ref('dim_person_pseudo') }} as p
    on a.person_id = p.person_id
left join {{ ref('stg_olids_practitioner_in_role') }} as practitioner
    on a.practitioner_in_role_id = practitioner.id
left join {{ ref('stg_olids_organisation') }} as provider
    on a.provider_organisation_id = provider.id
left join {{ ref('stg_olids_organisation') }} as publisher
    on a.publisher_organisation_id = publisher.id
left join {{ ref('stg_olids_schedule') }} as schedule
    on a.schedule_id = schedule.id
