{{
    config(
        materialized='table',
        tags=['cltcs_secure_source'])
}}


/*
Patient data processing for CLTCS

Clinical Purpose:
- Details LTCS summary data for patients with complex needs in C-LTCS

*/
{% set measurement_cutoff = -2 %}
with inclusion_list as (
    select patient_id, area_code, olids_id
    from {{ ref('cltcs_patient_list')}}
),
sus_uec_events as(
    select
        il.patient_id,
        il.area_code,
        sus_events.start_date as event_start_date,
        -- AE departure date is nullable; fall back to the arrival date so an
        -- attendance with no recorded departure is a same-day point event
        -- rather than a null that downstream stretches to "today".
        coalesce(sus_events.end_date, sus_events.start_date) as event_end_date,
        sus_events.organisation_name,
        sus_events.visit_occurrence_type as event_type,
        sus_events.pod as event_detail,
        sus_events.visit_occurrence_id::varchar as event_id,
        sus_events.end_date is null as is_end_date_imputed
    from {{ ref('int_sus_uec_encounter')}} sus_events
    inner join inclusion_list il on il.patient_id = sus_events.sk_patient_id
    where sus_events.start_date between dateadd(year, {{ measurement_cutoff }}, current_date()) and current_date()
),
sus_apc_events as(
    -- Use the imputed-spells model: it fills missing discharge dates from median
    -- duration (capped at today), dedupes spells and excludes births. The
    -- dq_end_date marker flags spells whose end date was originally missing so
    -- the app can render them distinctly. The model emits a rolling 2-year
    -- window (median imputation is still pinned to 12 months internally).
    select
        il.patient_id,
        il.area_code,
        sus_events.start_date as event_start_date,
        coalesce(sus_events.end_date, sus_events.start_date) as event_end_date,
        sus_events.organisation_name,
        sus_events.visit_occurrence_type as event_type,
        admission_method_name as event_detail,
        sus_events.visit_occurrence_id::varchar as event_id,
        sus_events.dq_end_date as is_end_date_imputed
    from {{ ref('int_sus_apc_imputed_spells')}} sus_events
    inner join inclusion_list il on il.patient_id = sus_events.sk_patient_id
    where sus_events.start_date between dateadd(year, {{ measurement_cutoff }}, current_date()) and current_date()
),
sus_op_events as(
    select
        il.patient_id,
        il.area_code,
        sus_events.start_date as event_start_date,
        sus_events.start_date as event_end_date,
        sus_events.organisation_name,
        sus_events.visit_occurrence_type as event_type,
        sus_events.pod as event_detail,
        sus_events.visit_occurrence_id::varchar as event_id,
        false as is_end_date_imputed
    from {{ ref('int_sus_op_encounter')}} sus_events
    inner join inclusion_list il on il.patient_id = sus_events.sk_patient_id
    where sus_events.start_date between dateadd(year, {{ measurement_cutoff }}, current_date()) and current_date()
),
gp_events as (
    select
        il.patient_id,
        il.area_code,
        gpa.start_date as event_start_date,
        gpa.start_date as event_end_date,
        dp.practice_name as organisation_name,
        'GP_APPT' as event_type,
        gpa.practitioner_role_group as event_detail,
        gpa.appointment_id::varchar as event_id,
        false as is_end_date_imputed
    from {{ ref('int_appointment_gp_clinical') }} gpa
    inner join inclusion_list il on il.olids_id = gpa.person_id
    left join {{ ref('dim_practice')}} dp on dp.organisation_id = gpa.provider_organisation_id
    where gpa.start_date between dateadd(year, {{ measurement_cutoff }}, current_date()) and current_date()
    and gpa.is_attended = TRUE
),

complete_events as (
    select * from sus_uec_events
    union all
    select * from sus_apc_events
    union all
    select * from sus_op_events
    union all
    select * from gp_events
  )

select patient_id,
    area_code,
    event_start_date,
    event_end_date,
    organisation_name,
    event_type,
    event_detail,
    event_id,
    is_end_date_imputed
from complete_events 