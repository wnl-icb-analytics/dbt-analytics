{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['source_record_type', 'source_record_id'],
    on_schema_change='fail',
    cluster_by=['sk_patient_id', 'coalesce(event_at, event_date::timestamp_ntz)'],
    tags=['healthcare_event_stream', 'daily'],
    pre_hook="{{ navigation_build_warehouse() }}",
    post_hook=["{{ navigation_remove_withdrawn_records([('referral', 'fct_mhsds_referral', 'source_record_id'), ('care_contact', 'fct_mhsds_care_contact', 'source_record_id'), ('hospital_provider_spell', 'fct_mhsds_hospital_provider_spell', 'source_record_id'), ('ward_stay', 'fct_mhsds_ward_stay', 'source_record_id')]) }}", "{{ navigation_build_warehouse(restore=true) }}"]
) }}

-- One recorded milestone per source record; the primary milestone retains unknown dates.
with source_records as (
select
    s.sk_patient_id::varchar as sk_patient_id,
    s.person_id::varchar as source_person_id,
    'referral'::varchar as source_record_type,
    s.source_record_id::varchar as source_record_id,
    'fct_mhsds_referral'::varchar as source_model_name,
    'mental_health'::varchar as care_setting,
    null::varchar as attendance_code,
    null::varchar as attendance_name,
    s.primary_service_or_team_type_code::varchar as service_or_team_type_code,
    s.primary_service_or_team_type_description::varchar as service_or_team_type_name,
    null::varchar as consultation_mechanism_code,
    null::varchar as consultation_mechanism_name,
    null::varchar as activity_location_type_code,
    null::varchar as activity_location_type_name,
    s.provider_organisation_code::varchar as provider_organisation_code,
    s.provider_organisation_name::varchar as provider_organisation_name,
    'ODS'::varchar as provider_code_authority,
    null::varchar as site_code,
    null::varchar as site_name,
    s.referring_organisation_code::varchar as referring_organisation_code,
    s.referring_organisation_name::varchar as referring_organisation_name,
    null::varchar as parent_record_type,
    null::varchar as parent_record_id,
    null::varchar as parent_model_name,
    null::varchar as relationship_type,
    s.reporting_period_end_date::date as source_submission_period,
    s.source_file_received_at::timestamp_ntz as source_received_at,
    array_construct(
        object_construct_keep_null(
            'type', 'referral_received', 'name', 'Referral received',
            'date', s.referral_received_date::date,
            'at', iff(s.referral_received_date is null, null, timestamp_ntz_from_parts(s.referral_received_date::date, s.referral_received_time::time)),
            'precision', iff(s.referral_received_date is null, 'unknown', iff(s.referral_received_time is null, 'date', 'timestamp')),
            'basis', 'referral_received_date', 'retain_undated', true
        ),
        object_construct_keep_null(
            'type', 'referral_rejected', 'name', 'Referral rejected',
            'date', s.referral_rejected_at::date,
            'at', iff(s.referral_rejected_at is null, null, iff(s.referral_rejected_time_precision = 'timestamp', s.referral_rejected_at, null)),
            'precision', iff(s.referral_rejected_at is null, 'unknown', coalesce(s.referral_rejected_time_precision, 'unknown')),
            'basis', 'referral_rejected_at', 'retain_undated', false
        ),
        object_construct_keep_null(
            'type', 'referral_discharged', 'name', 'Referral discharged',
            'date', s.referral_discharge_date::date,
            'at', iff(s.referral_discharge_date is null, null, timestamp_ntz_from_parts(s.referral_discharge_date::date, s.referral_discharge_time::time)),
            'precision', iff(s.referral_discharge_date is null, 'unknown', iff(s.referral_discharge_time is null, 'date', 'timestamp')),
            'basis', 'referral_discharge_date', 'retain_undated', false
        )
    ) as milestones
from {{ ref('fct_mhsds_referral') }} as s
where true
{{ navigation_delivery_filter('s.source_file_received_at', 'referral') }}
union all
select
    s.sk_patient_id::varchar as sk_patient_id,
    s.person_id::varchar as source_person_id,
    'care_contact'::varchar as source_record_type,
    s.source_record_id::varchar as source_record_id,
    'fct_mhsds_care_contact'::varchar as source_model_name,
    'mental_health'::varchar as care_setting,
    s.attendance_status_code::varchar as attendance_code,
    s.attendance_status_description::varchar as attendance_name,
    s.service_or_team_type_code::varchar as service_or_team_type_code,
    s.service_or_team_type_description::varchar as service_or_team_type_name,
    s.consultation_mechanism_code::varchar as consultation_mechanism_code,
    s.consultation_mechanism_description::varchar as consultation_mechanism_name,
    s.activity_location_type_code::varchar as activity_location_type_code,
    s.activity_location_type_description::varchar as activity_location_type_name,
    s.provider_organisation_code::varchar as provider_organisation_code,
    s.provider_organisation_name::varchar as provider_organisation_name,
    'ODS'::varchar as provider_code_authority,
    s.treatment_site_code::varchar as site_code,
    s.treatment_site_name::varchar as site_name,
    null::varchar as referring_organisation_code,
    null::varchar as referring_organisation_name,
    iff(s.referral_source_record_id is not null, 'referral', null)::varchar as parent_record_type,
    s.referral_source_record_id::varchar as parent_record_id,
    iff(s.referral_source_record_id is not null, 'fct_mhsds_referral', null)::varchar as parent_model_name,
    iff(s.referral_source_record_id is not null, 'recorded_parent', null)::varchar as relationship_type,
    s.reporting_period_end_date::date as source_submission_period,
    s.source_file_received_at::timestamp_ntz as source_received_at,
    array_construct(
        object_construct_keep_null(
            'type', 'care_contact', 'name', 'Care contact',
            'date', s.care_contact_date::date,
            'at', iff(s.care_contact_date is null, null, timestamp_ntz_from_parts(s.care_contact_date::date, s.care_contact_time::time)),
            'precision', iff(s.care_contact_date is null, 'unknown', iff(s.care_contact_time is null, 'date', 'timestamp')),
            'basis', 'care_contact_date', 'retain_undated', true
        )
    ) as milestones
from {{ ref('fct_mhsds_care_contact') }} as s
where true
{{ navigation_delivery_filter('s.source_file_received_at', 'care_contact') }}
union all
select
    s.sk_patient_id::varchar as sk_patient_id,
    s.person_id::varchar as source_person_id,
    'hospital_provider_spell'::varchar as source_record_type,
    s.source_record_id::varchar as source_record_id,
    'fct_mhsds_hospital_provider_spell'::varchar as source_model_name,
    'mental_health_inpatient'::varchar as care_setting,
    null::varchar as attendance_code,
    null::varchar as attendance_name,
    null::varchar as service_or_team_type_code,
    null::varchar as service_or_team_type_name,
    null::varchar as consultation_mechanism_code,
    null::varchar as consultation_mechanism_name,
    null::varchar as activity_location_type_code,
    null::varchar as activity_location_type_name,
    s.provider_organisation_code::varchar as provider_organisation_code,
    s.provider_organisation_name::varchar as provider_organisation_name,
    'ODS'::varchar as provider_code_authority,
    null::varchar as site_code,
    null::varchar as site_name,
    null::varchar as referring_organisation_code,
    null::varchar as referring_organisation_name,
    iff(s.referral_source_record_id is not null, 'referral', null)::varchar as parent_record_type,
    s.referral_source_record_id::varchar as parent_record_id,
    iff(s.referral_source_record_id is not null, 'fct_mhsds_referral', null)::varchar as parent_model_name,
    iff(s.referral_source_record_id is not null, 'recorded_parent', null)::varchar as relationship_type,
    s.reporting_period_end_date::date as source_submission_period,
    s.source_file_received_at::timestamp_ntz as source_received_at,
    array_construct(
        object_construct_keep_null(
            'type', 'admission', 'name', 'Hospital admission',
            'date', s.admission_date::date,
            'at', iff(s.admission_date is null, null, timestamp_ntz_from_parts(s.admission_date::date, s.admission_time::time)),
            'precision', iff(s.admission_date is null, 'unknown', iff(s.admission_time is null, 'date', 'timestamp')),
            'basis', 'admission_date', 'retain_undated', true
        ),
        object_construct_keep_null(
            'type', 'discharge', 'name', 'Hospital discharge',
            'date', s.discharge_date::date,
            'at', iff(s.discharge_date is null, null, timestamp_ntz_from_parts(s.discharge_date::date, s.discharge_time::time)),
            'precision', iff(s.discharge_date is null, 'unknown', iff(s.discharge_time is null, 'date', 'timestamp')),
            'basis', 'discharge_date', 'retain_undated', false
        )
    ) as milestones
from {{ ref('fct_mhsds_hospital_provider_spell') }} as s
where true
{{ navigation_delivery_filter('s.source_file_received_at', 'hospital_provider_spell') }}
union all
select
    s.sk_patient_id::varchar as sk_patient_id,
    s.person_id::varchar as source_person_id,
    'ward_stay'::varchar as source_record_type,
    s.source_record_id::varchar as source_record_id,
    'fct_mhsds_ward_stay'::varchar as source_model_name,
    'mental_health_inpatient'::varchar as care_setting,
    null::varchar as attendance_code,
    null::varchar as attendance_name,
    null::varchar as service_or_team_type_code,
    null::varchar as service_or_team_type_name,
    null::varchar as consultation_mechanism_code,
    null::varchar as consultation_mechanism_name,
    null::varchar as activity_location_type_code,
    null::varchar as activity_location_type_name,
    s.provider_organisation_code::varchar as provider_organisation_code,
    s.provider_organisation_name::varchar as provider_organisation_name,
    'ODS'::varchar as provider_code_authority,
    null::varchar as site_code,
    null::varchar as site_name,
    null::varchar as referring_organisation_code,
    null::varchar as referring_organisation_name,
    iff(s.hospital_provider_spell_source_record_id is not null, 'hospital_provider_spell', null)::varchar as parent_record_type,
    s.hospital_provider_spell_source_record_id::varchar as parent_record_id,
    iff(s.hospital_provider_spell_source_record_id is not null, 'fct_mhsds_hospital_provider_spell', null)::varchar as parent_model_name,
    iff(s.hospital_provider_spell_source_record_id is not null, 'recorded_parent', null)::varchar as relationship_type,
    s.reporting_period_end_date::date as source_submission_period,
    s.source_file_received_at::timestamp_ntz as source_received_at,
    array_construct(
        object_construct_keep_null(
            'type', 'ward_stay_started', 'name', 'Ward stay started',
            'date', s.ward_stay_start_date::date,
            'at', iff(s.ward_stay_start_date is null, null, timestamp_ntz_from_parts(s.ward_stay_start_date::date, s.ward_stay_start_time::time)),
            'precision', iff(s.ward_stay_start_date is null, 'unknown', iff(s.ward_stay_start_time is null, 'date', 'timestamp')),
            'basis', 'ward_stay_start_date', 'retain_undated', true
        ),
        object_construct_keep_null(
            'type', 'ward_stay_ended', 'name', 'Ward stay ended',
            'date', s.ward_stay_end_date::date,
            'at', iff(s.ward_stay_end_date is null, null, timestamp_ntz_from_parts(s.ward_stay_end_date::date, s.ward_stay_end_time::time)),
            'precision', iff(s.ward_stay_end_date is null, 'unknown', iff(s.ward_stay_end_time is null, 'date', 'timestamp')),
            'basis', 'ward_stay_end_date', 'retain_undated', false
        )
    ) as milestones
from {{ ref('fct_mhsds_ward_stay') }} as s
where true
{{ navigation_delivery_filter('s.source_file_received_at', 'ward_stay') }}
)
select
    {{ dbt_utils.generate_surrogate_key(["'MHSDS'", 'r.source_record_type', 'r.source_record_id', 'm.value:type::varchar']) }} as event_id,
    r.sk_patient_id as sk_patient_id,
    r.source_person_id as source_person_id,
    'MHSDS' as source_dataset,
    r.source_record_type as source_record_type,
    r.source_record_id as source_record_id,
    r.source_model_name as source_model_name,
    m.value:date::date as event_date,
    iff(m.value:precision::varchar = 'timestamp', m.value:at::timestamp_ntz, null) as event_at,
    m.value:precision::varchar as event_time_precision,
    m.value:basis::varchar as event_time_basis,
    m.value:type::varchar as event_type,
    m.value:name::varchar as event_name,
    r.care_setting as care_setting,
    r.attendance_code as attendance_code,
    r.attendance_name as attendance_name,
    r.service_or_team_type_code as service_or_team_type_code,
    r.service_or_team_type_name as service_or_team_type_name,
    r.consultation_mechanism_code as consultation_mechanism_code,
    r.consultation_mechanism_name as consultation_mechanism_name,
    r.activity_location_type_code as activity_location_type_code,
    r.activity_location_type_name as activity_location_type_name,
    r.provider_organisation_code as provider_organisation_code,
    r.provider_organisation_name as provider_organisation_name,
    r.provider_code_authority as provider_code_authority,
    r.site_code as site_code,
    r.site_name as site_name,
    r.referring_organisation_code as referring_organisation_code,
    r.referring_organisation_name as referring_organisation_name,
    r.parent_record_type as parent_record_type,
    r.parent_record_id as parent_record_id,
    r.parent_model_name as parent_model_name,
    r.relationship_type as relationship_type,
    r.source_submission_period as source_submission_period,
    r.source_received_at as source_received_at
from source_records as r, lateral flatten(input => r.milestones) as m
where m.value:retain_undated::boolean or m.value:date::date is not null
