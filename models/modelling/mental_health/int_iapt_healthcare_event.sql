{{ config(
    cluster_by=['sk_patient_id', 'event_at'],
    tags=['healthcare_event_stream']
) }}

-- One recorded milestone per source record; the primary milestone retains unknown dates.
-- A referral parent is kept only when it exists and names the same person.
with source_records as (
select
    s.sk_patient_id::varchar as sk_patient_id,
    s.person_id::varchar as source_person_id,
    'referral'::varchar as source_record_type,
    s.source_record_id::varchar as source_record_id,
    'fct_iapt_referral'::varchar as source_model_name,
    null::varchar as event_code,
    null::varchar as event_code_name,
    null::varchar as event_coding_system,
    null::varchar as attendance_code,
    null::varchar as attendance_name,
    null::varchar as consultation_mechanism_code,
    null::varchar as consultation_mechanism_name,
    null::varchar as activity_location_type_code,
    null::varchar as activity_location_type_name,
    s.provider_organisation_code::varchar as provider_organisation_code,
    s.provider_organisation_name::varchar as provider_organisation_name,
    null::varchar as site_code,
    null::varchar as site_name,
    null::varchar as parent_record_id,
    s.reporting_period_end_date::date as source_submission_period,
    s.source_loaded_at::timestamp_ntz as source_received_at,
    array_construct(
        object_construct_keep_null(
            'type', 'referral_received', 'name', 'Referral received',
            'date', s.referral_received_date::date, 'at', null,
            'precision', iff(s.referral_received_date is null, 'unknown', 'date'),
            'basis', 'referral_received_date', 'retain_undated', true,
            'outcome_code', null, 'outcome_name', null
        ),
        object_construct_keep_null(
            'type', 'referral_discharged', 'name', 'Referral discharged',
            'date', s.service_discharge_date::date, 'at', null,
            'precision', iff(s.service_discharge_date is null, 'unknown', 'date'),
            'basis', 'service_discharge_date', 'retain_undated', false,
            'outcome_code', s.discharge_reason_code, 'outcome_name', s.discharge_reason_name
        )
    ) as milestones
from {{ ref('fct_iapt_referral') }} as s

union all

select
    s.sk_patient_id::varchar,
    s.person_id::varchar,
    'care_contact'::varchar,
    s.source_record_id::varchar,
    'fct_iapt_care_contact'::varchar,
    s.appointment_type_code::varchar,
    s.appointment_type_name::varchar,
    iff(s.appointment_type_code is not null, 'IAPT appointment type', null)::varchar,
    s.attendance_code::varchar,
    s.attendance_name::varchar,
    s.consultation_mechanism_code::varchar,
    s.consultation_mechanism_name::varchar,
    s.activity_location_type_code::varchar,
    s.activity_location_type_name::varchar,
    s.provider_organisation_code::varchar,
    s.provider_organisation_name::varchar,
    s.site_code::varchar,
    s.site_name::varchar,
    iff(s.is_referral_linked and s.is_referral_person_consistent, s.referral_id, null)::varchar,
    s.reporting_period_end_date::date,
    s.source_loaded_at::timestamp_ntz,
    array_construct(
        object_construct_keep_null(
            'type', 'care_contact', 'name', 'Care contact',
            'date', s.care_contact_date::date, 'at', s.care_contact_at::timestamp_ntz,
            'precision', coalesce(s.care_contact_time_precision, 'unknown'),
            'basis', 'care_contact_date', 'retain_undated', true,
            'outcome_code', null, 'outcome_name', null
        )
    )
from {{ ref('fct_iapt_care_contact') }} as s

union all

select
    s.sk_patient_id::varchar,
    s.person_id::varchar,
    'onward_referral'::varchar,
    s.source_record_id::varchar,
    'fct_iapt_onward_referral'::varchar,
    null::varchar,
    null::varchar,
    null::varchar,
    null::varchar,
    null::varchar,
    null::varchar,
    null::varchar,
    null::varchar,
    null::varchar,
    s.provider_organisation_code::varchar,
    s.provider_organisation_name::varchar,
    null::varchar,
    null::varchar,
    iff(s.is_referral_linked and s.is_referral_person_consistent, s.referral_id, null)::varchar,
    s.reporting_period_end_date::date,
    s.source_loaded_at::timestamp_ntz,
    array_construct(
        object_construct_keep_null(
            'type', 'onward_referral', 'name', 'Onward referral',
            'date', s.onward_referral_date::date, 'at', s.onward_referral_at::timestamp_ntz,
            'precision', coalesce(s.onward_referral_time_precision, 'unknown'),
            'basis', 'onward_referral_date', 'retain_undated', true,
            'outcome_code', s.onward_referral_reason_code, 'outcome_name', s.onward_referral_reason_name
        )
    )
from {{ ref('fct_iapt_onward_referral') }} as s
)

select
    {{ dbt_utils.generate_surrogate_key(["'IAPT'", 'r.source_record_type', 'r.source_record_id', 'm.value:type::varchar']) }} as event_id,
    r.sk_patient_id,
    r.source_person_id,
    'IAPT'::varchar as source_dataset,
    r.source_record_type,
    r.source_record_id,
    r.source_model_name,
    m.value:date::date as event_date,
    -- Midnight is a sort anchor for date-only rows, not an observed time.
    iff(m.value:precision::varchar = 'timestamp', m.value:at::timestamp_ntz, m.value:date::date::timestamp_ntz) as event_at,
    m.value:precision::varchar as event_time_precision,
    m.value:basis::varchar as event_time_basis,
    m.value:type::varchar as event_type,
    m.value:name::varchar as event_name,
    r.event_code,
    r.event_code_name,
    r.event_coding_system,
    'mental_health'::varchar as care_setting,
    r.attendance_code,
    r.attendance_name,
    m.value:outcome_code::varchar as outcome_code,
    m.value:outcome_name::varchar as outcome_name,
    r.consultation_mechanism_code,
    r.consultation_mechanism_name,
    r.activity_location_type_code,
    r.activity_location_type_name,
    r.provider_organisation_code,
    r.provider_organisation_name,
    'ODS'::varchar as provider_code_authority,
    r.site_code,
    r.site_name,
    iff(r.parent_record_id is not null, 'referral', null)::varchar as parent_record_type,
    r.parent_record_id,
    iff(r.parent_record_id is not null, 'fct_iapt_referral', null)::varchar as parent_model_name,
    iff(r.parent_record_id is not null, 'recorded_parent', null)::varchar as relationship_type,
    r.source_submission_period,
    r.source_received_at
from source_records as r, lateral flatten(input => r.milestones) as m
where m.value:retain_undated::boolean or m.value:date::date is not null
