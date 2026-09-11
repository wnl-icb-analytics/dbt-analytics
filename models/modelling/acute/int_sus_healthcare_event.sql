{{ config(
    materialized='incremental', incremental_strategy='delete+insert',
    unique_key=['source_record_type', 'source_record_id'], on_schema_change='fail',
    cluster_by=['sk_patient_id', 'coalesce(event_at, event_date::timestamp_ntz)'], tags=['healthcare_event_stream', 'daily'],
    pre_hook="{{ navigation_build_warehouse() }}",
    post_hook=["{{ navigation_remove_withdrawn_records([('apc_spell', 'obt_encounter_apc', 'visit_occurrence_id'), ('outpatient_appointment', 'int_sus_op_appointment', 'visit_occurrence_id'), ('emergency_care_attendance', 'obt_encounter_uec', 'visit_occurrence_id')]) }}", "{{ navigation_build_warehouse(restore=true) }}"]
) }}

with source_records as (
select
    'SUS_APC'::varchar as source_dataset,
    'apc_spell'::varchar as source_record_type,
    s.visit_occurrence_id::varchar as source_record_id,
    'obt_encounter_apc'::varchar as source_model_name,
    s.sk_patient_id::varchar as sk_patient_id,
    'inpatient'::varchar as care_setting,
    s.organisation_id::varchar as provider_organisation_code,
    s.organisation_name::varchar as provider_organisation_name,
    s.site_id::varchar as site_code,
    s.site_name::varchar as site_name,
    s.main_specialty_code::varchar as specialty_code,
    s.main_specialty_name::varchar as specialty_name,
    null::varchar as attendance_code,
    null::varchar as attendance_name,
    null::varchar as outcome_code,
    null::varchar as outcome_name,
    delivery.source_received_at::timestamp_ntz as source_received_at,
    array_construct(
        object_construct_keep_null('type', 'admission', 'name', 'Hospital admission', 'date', s.start_date::date, 'at', timestamp_ntz_from_parts(s.start_date::date, s.start_time::time), 'precision', iff(s.start_date is null, 'unknown', iff(s.start_time is null, 'date', 'timestamp')), 'basis', 'start_date', 'retain_undated', true),
        object_construct_keep_null('type', 'discharge', 'name', 'Hospital discharge', 'date', s.end_date::date, 'at', timestamp_ntz_from_parts(s.end_date::date, s.end_time::time), 'precision', iff(s.end_date is null, 'unknown', iff(s.end_time is null, 'date', 'timestamp')), 'basis', 'end_date', 'retain_undated', false)
    ) as milestones
from {{ ref('obt_encounter_apc') }} as s
left join {{ ref('stg_sus_apc_spell') }} as delivery on s.visit_occurrence_id = delivery.primarykey_id
where true
{{ navigation_delivery_filter('delivery.source_received_at', 'apc_spell') }}
union all
select
    'SUS_OP'::varchar as source_dataset,
    'outpatient_appointment'::varchar as source_record_type,
    s.visit_occurrence_id::varchar as source_record_id,
    'int_sus_op_appointment'::varchar as source_model_name,
    s.sk_patient_id::varchar as sk_patient_id,
    'outpatient'::varchar as care_setting,
    s.organisation_id::varchar as provider_organisation_code,
    s.organisation_name::varchar as provider_organisation_name,
    s.site_id::varchar as site_code,
    s.site_name::varchar as site_name,
    s.main_specialty_code::varchar as specialty_code,
    s.main_specialty_name::varchar as specialty_name,
    s.appointment_attended_or_dna::varchar as attendance_code,
    s.appointment_attendance_outcome_desc::varchar as attendance_name,
    s.appointment_outcome::varchar as outcome_code,
    s.outcome_desc::varchar as outcome_name,
    delivery.source_received_at::timestamp_ntz as source_received_at,
    array_construct(
        object_construct_keep_null('type', 'appointment_slot', 'name', 'Scheduled appointment', 'date', s.start_date::date, 'at', timestamp_ntz_from_parts(s.start_date::date, s.start_time::time), 'precision', iff(s.start_date is null, 'unknown', iff(s.start_time is null, 'date', 'timestamp')), 'basis', 'start_date', 'retain_undated', true)
    ) as milestones
from {{ ref('int_sus_op_appointment') }} as s
left join {{ ref('stg_sus_op_appointment') }} as delivery on s.visit_occurrence_id = delivery.primarykey_id
where true
{{ navigation_delivery_filter('delivery.source_received_at', 'outpatient_appointment') }}
union all
select
    'ECDS'::varchar as source_dataset,
    'emergency_care_attendance'::varchar as source_record_type,
    s.visit_occurrence_id::varchar as source_record_id,
    'obt_encounter_uec'::varchar as source_model_name,
    s.sk_patient_id::varchar as sk_patient_id,
    'urgent_emergency_care'::varchar as care_setting,
    s.organisation_id::varchar as provider_organisation_code,
    s.organisation_name::varchar as provider_organisation_name,
    s.site_id::varchar as site_code,
    s.site_name::varchar as site_name,
    s.main_specialty_code::varchar as specialty_code,
    s.main_specialty_name::varchar as specialty_name,
    null::varchar as attendance_code,
    null::varchar as attendance_name,
    null::varchar as outcome_code,
    null::varchar as outcome_name,
    delivery.source_received_at::timestamp_ntz as source_received_at,
    array_construct(
        object_construct_keep_null('type', 'emergency_care_arrival', 'name', 'Emergency care arrival', 'date', s.start_date::date, 'at', timestamp_ntz_from_parts(s.start_date::date, s.start_time::time), 'precision', iff(s.start_date is null, 'unknown', iff(s.start_time is null, 'date', 'timestamp')), 'basis', 'start_date', 'retain_undated', true),
        object_construct_keep_null('type', 'initial_assessment', 'name', 'Initial assessment', 'date', s.initial_assessment_date::date, 'at', timestamp_ntz_from_parts(s.initial_assessment_date::date, s.initial_assessment_time::time), 'precision', iff(s.initial_assessment_date is null, 'unknown', iff(s.initial_assessment_time is null, 'date', 'timestamp')), 'basis', 'initial_assessment_date', 'retain_undated', false),
        object_construct_keep_null('type', 'seen_for_treatment', 'name', 'Seen for treatment', 'date', s.seen_for_treatment_date::date, 'at', timestamp_ntz_from_parts(s.seen_for_treatment_date::date, s.seen_for_treatment_time::time), 'precision', iff(s.seen_for_treatment_date is null, 'unknown', iff(s.seen_for_treatment_time is null, 'date', 'timestamp')), 'basis', 'seen_for_treatment_date', 'retain_undated', false),
        object_construct_keep_null('type', 'decision_to_admit', 'name', 'Decision to admit', 'date', s.decided_to_admit_date::date, 'at', timestamp_ntz_from_parts(s.decided_to_admit_date::date, s.decided_to_admit_time::time), 'precision', iff(s.decided_to_admit_date is null, 'unknown', iff(s.decided_to_admit_time is null, 'date', 'timestamp')), 'basis', 'decided_to_admit_date', 'retain_undated', false),
        object_construct_keep_null('type', 'emergency_care_departure', 'name', 'Emergency care departure', 'date', s.end_date::date, 'at', timestamp_ntz_from_parts(s.end_date::date, s.end_time::time), 'precision', iff(s.end_date is null, 'unknown', iff(s.end_time is null, 'date', 'timestamp')), 'basis', 'end_date', 'retain_undated', false)
    ) as milestones
from {{ ref('obt_encounter_uec') }} as s
left join {{ ref('stg_sus_ecds_emergency_care') }} as delivery on s.visit_occurrence_id = delivery.primarykey_id
where true
{{ navigation_delivery_filter('delivery.source_received_at', 'emergency_care_attendance') }}
)
select
    {{ dbt_utils.generate_surrogate_key(['r.source_dataset', 'r.source_record_type', 'r.source_record_id', 'm.value:type::varchar']) }} as event_id,
    r.source_dataset as source_dataset,
    r.source_record_type as source_record_type,
    r.source_record_id as source_record_id,
    r.source_model_name as source_model_name,
    r.sk_patient_id as sk_patient_id,
    r.care_setting as care_setting,
    r.provider_organisation_code as provider_organisation_code,
    r.provider_organisation_name as provider_organisation_name,
    r.site_code as site_code,
    r.site_name as site_name,
    r.specialty_code as specialty_code,
    r.specialty_name as specialty_name,
    r.attendance_code as attendance_code,
    r.attendance_name as attendance_name,
    r.outcome_code as outcome_code,
    r.outcome_name as outcome_name,
    r.source_received_at as source_received_at,
    m.value:type::varchar as event_type,
    m.value:name::varchar as event_name,
    m.value:date::date as event_date,
    iff(m.value:precision::varchar = 'timestamp', m.value:at::timestamp_ntz, null) as event_at,
    m.value:precision::varchar as event_time_precision,
    m.value:basis::varchar as event_time_basis,
    'ODS'::varchar as provider_code_authority
from source_records as r, lateral flatten(input => r.milestones) as m
where m.value:retain_undated::boolean or m.value:date::date is not null
