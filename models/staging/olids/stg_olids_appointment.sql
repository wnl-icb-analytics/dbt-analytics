select
    -- Primary key
    id,

    -- Business columns
    provider_organisation_id,
    patient_id,
    person_id,
    practitioner_in_role_id,
    schedule_id,
    start_date,
    planned_duration_mins,
    actual_duration_mins,
    appointment_status_source_concept_id as status_source_concept_id,
    appointment_status_code,
    appointment_status_display,
    appointment_status_source_code,
    appointment_status_source_display,
    patient_wait_mins,
    patient_delay_mins,
    datetime_booked,
    datetime_sent_in,
    datetime_left,
    cancelled_date,
    appointment_type,
    age_at_event,
    age_at_event_baby,
    age_at_event_neonate,
    booking_method_source_concept_id,
    booking_method_code,
    booking_method_display,
    booking_method_source_code,
    booking_method_source_display,
    contact_mode_source_concept_id,
    contact_mode_code,
    contact_mode_display,
    contact_mode_source_code,
    contact_mode_source_display,
    is_blocked,
    national_slot_category_name,
    context_type,
    service_setting,
    national_slot_category_description,
    csds_care_contact_identifier,
    publisher_organisation_code,
    lds_transform_datetime,

    -- Metadata
    lds_is_deleted,
    lds_source_record_id,


    -- New columns exposed by the 2026 OLIDS schema realignment (issue #747)
    publisher_organisation_id
from {{ ref('raw_olids_appointment') }}
where coalesce(lds_is_deleted, false) = false
    and person_id is not null
