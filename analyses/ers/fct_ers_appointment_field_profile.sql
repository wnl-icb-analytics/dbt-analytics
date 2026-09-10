-- Whole-table column coverage; no identifiers or patient records are returned.
with stats as (
    select object_construct(
        'appointment_id', object_construct('populated', count(appointment_id), 'blank', count_if(trim(appointment_id::varchar) = '')),
        'ubrn_id', object_construct('populated', count(ubrn_id), 'blank', count_if(trim(ubrn_id::varchar) = '')),
        'ubrn', object_construct('populated', count(ubrn), 'blank', count_if(trim(ubrn::varchar) = '')),
        'sk_patient_id', object_construct('populated', count(sk_patient_id), 'blank', count_if(trim(sk_patient_id::varchar) = '')),
        'appointment_at', object_construct('populated', count(appointment_at), 'blank', count_if(trim(appointment_at::varchar) = '')),
        'service_id', object_construct('populated', count(service_id), 'blank', count_if(trim(service_id::varchar) = '')),
        'service_name', object_construct('populated', count(service_name), 'blank', count_if(trim(service_name::varchar) = '')),
        'service_specialty_code', object_construct('populated', count(service_specialty_code), 'blank', count_if(trim(service_specialty_code::varchar) = '')),
        'service_specialty_name', object_construct('populated', count(service_specialty_name), 'blank', count_if(trim(service_specialty_name::varchar) = '')),
        'provider_organisation_code', object_construct('populated', count(provider_organisation_code), 'blank', count_if(trim(provider_organisation_code::varchar) = '')),
        'provider_organisation_name', object_construct('populated', count(provider_organisation_name), 'blank', count_if(trim(provider_organisation_name::varchar) = '')),
        'site_code', object_construct('populated', count(site_code), 'blank', count_if(trim(site_code::varchar) = '')),
        'site_name', object_construct('populated', count(site_name), 'blank', count_if(trim(site_name::varchar) = '')),
        'appointment_type_code', object_construct('populated', count(appointment_type_code), 'blank', count_if(trim(appointment_type_code::varchar) = '')),
        'appointment_type_name', object_construct('populated', count(appointment_type_name), 'blank', count_if(trim(appointment_type_name::varchar) = '')),
        'action_count', object_construct('populated', count(action_count), 'blank', count_if(trim(action_count::varchar) = '')),
        'recorded_booking_action_count', object_construct('populated', count(recorded_booking_action_count), 'blank', count_if(trim(recorded_booking_action_count::varchar) = '')),
        'first_observed_action_at', object_construct('populated', count(first_observed_action_at), 'blank', count_if(trim(first_observed_action_at::varchar) = '')),
        'last_observed_action_at', object_construct('populated', count(last_observed_action_at), 'blank', count_if(trim(last_observed_action_at::varchar) = '')),
        'latest_action_id', object_construct('populated', count(latest_action_id), 'blank', count_if(trim(latest_action_id::varchar) = '')),
        'latest_appointment_action_id', object_construct('populated', count(latest_appointment_action_id), 'blank', count_if(trim(latest_appointment_action_id::varchar) = '')),
        'latest_appointment_action_at', object_construct('populated', count(latest_appointment_action_at), 'blank', count_if(trim(latest_appointment_action_at::varchar) = '')),
        'latest_appointment_action_code', object_construct('populated', count(latest_appointment_action_code), 'blank', count_if(trim(latest_appointment_action_code::varchar) = '')),
        'latest_appointment_action_name', object_construct('populated', count(latest_appointment_action_name), 'blank', count_if(trim(latest_appointment_action_name::varchar) = '')),
        'latest_appointment_action_reason_code', object_construct('populated', count(latest_appointment_action_reason_code), 'blank', count_if(trim(latest_appointment_action_reason_code::varchar) = '')),
        'latest_appointment_action_reason_name', object_construct('populated', count(latest_appointment_action_reason_name), 'blank', count_if(trim(latest_appointment_action_reason_name::varchar) = ''))
    ) as fields
    from {{ ref('fct_ers_appointment') }}
)
select f.key as column_name, f.value as coverage
from stats, lateral flatten(input => fields) f
