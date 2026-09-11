{{ config(
    materialized='incremental', incremental_strategy='delete+insert',
    unique_key=['source_record_type', 'source_record_id'], on_schema_change='fail',
    cluster_by=['sk_patient_id', 'coalesce(event_at, event_date::timestamp_ntz)'], tags=['healthcare_event_stream', 'daily'],
    pre_hook="{{ navigation_build_warehouse() }}",
    post_hook=[
        "{{ navigation_remove_withdrawn_records([('referral_action', 'fct_ers_referral_action', 'action_id'), ('appointment', 'fct_ers_appointment', 'appointment_id')]) }}",
        "{{ ers_remove_non_lifecycle_events() }}",
        "{{ navigation_build_warehouse(restore=true) }}"
    ]
) }}

with milestones as (
    select
        a.sk_patient_id,
        'referral_action'::varchar as source_record_type,
        a.action_id::varchar as source_record_id,
        'fct_ers_referral_action'::varchar as source_model_name,
        a.action_at as event_at,
        'action_at'::varchar as event_time_basis,
        mapping.event_type,
        a.action_name as event_name,
        a.action_code as event_code,
        a.action_name as event_code_name,
        'e-RS action'::varchar as event_coding_system,
        a.action_reason_code as outcome_code,
        a.action_reason_name as outcome_name,
        a.specialty_code,
        a.specialty_name,
        a.provider_organisation_code,
        a.provider_organisation_name,
        a.site_code,
        a.site_name,
        a.referring_organisation_code,
        a.referring_organisation_name,
        a.ubrn_id::varchar as parent_record_id,
        a.source_imported_at as source_received_at
    from {{ ref('fct_ers_referral_action') }} as a
    inner join {{ ref('ers_healthcare_event_type') }} as mapping
        on a.action_code = mapping.action_code
    where true
    {{ navigation_delivery_filter('a.source_imported_at', 'referral_action') }}

    union all

    select
        a.sk_patient_id,
        'appointment'::varchar as source_record_type,
        a.appointment_id::varchar as source_record_id,
        'fct_ers_appointment'::varchar as source_model_name,
        a.appointment_at as event_at,
        'appointment_at'::varchar as event_time_basis,
        'appointment_slot'::varchar as event_type,
        'Scheduled appointment'::varchar as event_name,
        a.latest_appointment_action_code as event_code,
        a.latest_appointment_action_name as event_code_name,
        'e-RS action'::varchar as event_coding_system,
        a.latest_appointment_action_reason_code as outcome_code,
        a.latest_appointment_action_reason_name as outcome_name,
        a.service_specialty_code as specialty_code,
        a.service_specialty_name as specialty_name,
        a.provider_organisation_code,
        a.provider_organisation_name,
        a.site_code,
        a.site_name,
        null::varchar as referring_organisation_code,
        null::varchar as referring_organisation_name,
        a.ubrn_id::varchar as parent_record_id,
        latest.source_imported_at as source_received_at
    from {{ ref('fct_ers_appointment') }} as a
    inner join {{ ref('fct_ers_referral_action') }} as latest
        on a.latest_action_id = latest.action_id
    where true
    {{ navigation_delivery_filter('latest.source_imported_at', 'appointment') }}
)
select
    {{ dbt_utils.generate_surrogate_key(["'ERS'", 'source_record_type', 'source_record_id', 'event_type']) }} as event_id,
    sk_patient_id,
    'ERS'::varchar as source_dataset,
    source_record_type,
    source_record_id,
    source_model_name,
    event_at::date as event_date,
    event_at,
    iff(event_at is null, 'unknown', 'timestamp')::varchar as event_time_precision,
    event_time_basis,
    event_type,
    event_name,
    event_code,
    event_code_name,
    event_coding_system,
    outcome_code,
    outcome_name,
    specialty_code,
    specialty_name,
    provider_organisation_code,
    provider_organisation_name,
    case
        when ods.organisation_code is not null then 'ODS'
        when provider_organisation_code is not null then 'e-RS'
    end as provider_code_authority,
    site_code,
    site_name,
    referring_organisation_code,
    referring_organisation_name,
    parent_record_id,
    'referral'::varchar as parent_record_type,
    'fct_ers_referral'::varchar as parent_model_name,
    'recorded_parent'::varchar as relationship_type,
    source_received_at
from milestones
left join {{ ref('organisation') }} as ods
    on milestones.provider_organisation_code = ods.organisation_code
