-- Returns only whole-table column coverage.
with stats as (
    select object_construct(
        'ubrn_id', object_construct('populated', count(ubrn_id), 'blank', count_if(trim(ubrn_id::varchar) = '')),
        'ubrn', object_construct('populated', count(ubrn), 'blank', count_if(trim(ubrn::varchar) = '')),
        'normalised_ubrn', object_construct('populated', count(normalised_ubrn), 'blank', count_if(trim(normalised_ubrn::varchar) = '')),
        'sk_patient_id', object_construct('populated', count(sk_patient_id), 'blank', count_if(trim(sk_patient_id::varchar) = '')),
        'pathway_started_at', object_construct('populated', count(pathway_started_at), 'blank', count_if(trim(pathway_started_at::varchar) = '')),
        'first_observed_action_at', object_construct('populated', count(first_observed_action_at), 'blank', count_if(trim(first_observed_action_at::varchar) = '')),
        'last_observed_action_at', object_construct('populated', count(last_observed_action_at), 'blank', count_if(trim(last_observed_action_at::varchar) = '')),
        'action_count', object_construct('populated', count(action_count), 'blank', count_if(trim(action_count::varchar) = '')),
        'first_action_id', object_construct('populated', count(first_action_id), 'blank', count_if(trim(first_action_id::varchar) = '')),
        'latest_action_id', object_construct('populated', count(latest_action_id), 'blank', count_if(trim(latest_action_id::varchar) = '')),
        'latest_action_code', object_construct('populated', count(latest_action_code), 'blank', count_if(trim(latest_action_code::varchar) = '')),
        'latest_action_name', object_construct('populated', count(latest_action_name), 'blank', count_if(trim(latest_action_name::varchar) = '')),
        'latest_action_reason_code', object_construct('populated', count(latest_action_reason_code), 'blank', count_if(trim(latest_action_reason_code::varchar) = '')),
        'latest_action_reason_name', object_construct('populated', count(latest_action_reason_name), 'blank', count_if(trim(latest_action_reason_name::varchar) = '')),
        'priority_code', object_construct('populated', count(priority_code), 'blank', count_if(trim(priority_code::varchar) = '')),
        'priority_name', object_construct('populated', count(priority_name), 'blank', count_if(trim(priority_name::varchar) = '')),
        'specialty_code', object_construct('populated', count(specialty_code), 'blank', count_if(trim(specialty_code::varchar) = '')),
        'specialty_name', object_construct('populated', count(specialty_name), 'blank', count_if(trim(specialty_name::varchar) = '')),
        'clinic_type_code', object_construct('populated', count(clinic_type_code), 'blank', count_if(trim(clinic_type_code::varchar) = '')),
        'clinic_type_name', object_construct('populated', count(clinic_type_name), 'blank', count_if(trim(clinic_type_name::varchar) = '')),
        'referring_organisation_code', object_construct('populated', count(referring_organisation_code), 'blank', count_if(trim(referring_organisation_code::varchar) = '')),
        'referring_organisation_name', object_construct('populated', count(referring_organisation_name), 'blank', count_if(trim(referring_organisation_name::varchar) = '')),
        'recorded_service_count', object_construct('populated', count(recorded_service_count), 'blank', count_if(trim(recorded_service_count::varchar) = '')),
        'last_service_action_id', object_construct('populated', count(last_service_action_id), 'blank', count_if(trim(last_service_action_id::varchar) = '')),
        'last_service_action_at', object_construct('populated', count(last_service_action_at), 'blank', count_if(trim(last_service_action_at::varchar) = '')),
        'last_recorded_service_id', object_construct('populated', count(last_recorded_service_id), 'blank', count_if(trim(last_recorded_service_id::varchar) = '')),
        'last_recorded_service_name', object_construct('populated', count(last_recorded_service_name), 'blank', count_if(trim(last_recorded_service_name::varchar) = '')),
        'last_recorded_service_specialty_code', object_construct('populated', count(last_recorded_service_specialty_code), 'blank', count_if(trim(last_recorded_service_specialty_code::varchar) = '')),
        'last_recorded_service_specialty_name', object_construct('populated', count(last_recorded_service_specialty_name), 'blank', count_if(trim(last_recorded_service_specialty_name::varchar) = '')),
        'last_recorded_provider_code', object_construct('populated', count(last_recorded_provider_code), 'blank', count_if(trim(last_recorded_provider_code::varchar) = '')),
        'last_recorded_provider_name', object_construct('populated', count(last_recorded_provider_name), 'blank', count_if(trim(last_recorded_provider_name::varchar) = '')),
        'last_recorded_site_code', object_construct('populated', count(last_recorded_site_code), 'blank', count_if(trim(last_recorded_site_code::varchar) = '')),
        'last_recorded_site_name', object_construct('populated', count(last_recorded_site_name), 'blank', count_if(trim(last_recorded_site_name::varchar) = '')),
        'initial_ubrn_id', object_construct('populated', count(initial_ubrn_id), 'blank', count_if(trim(initial_ubrn_id::varchar) = '')),
        'previous_ubrn_id', object_construct('populated', count(previous_ubrn_id), 'blank', count_if(trim(previous_ubrn_id::varchar) = '')),
        'initial_ubrn', object_construct('populated', count(initial_ubrn), 'blank', count_if(trim(initial_ubrn::varchar) = '')),
        'previous_ubrn', object_construct('populated', count(previous_ubrn), 'blank', count_if(trim(previous_ubrn::varchar) = '')),
        'next_ubrn', object_construct('populated', count(next_ubrn), 'blank', count_if(trim(next_ubrn::varchar) = ''))
    ) as fields
    from {{ ref('fct_ers_referral') }}
)
select f.key as column_name, f.value as coverage
from stats, lateral flatten(input => fields) f
