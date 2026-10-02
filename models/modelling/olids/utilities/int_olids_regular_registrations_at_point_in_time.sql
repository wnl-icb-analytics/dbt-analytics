{{
    config(
        materialized='table',
        tags=['intermediate', 'registration', 'patient', 'practice', 'emis_comparison']
    )
}}

with emis_extract as (
    -- The snapshot runs each morning on the previous night's OLIDS data, so the
    -- list observed the day after the extract matches the extract date.
    select
        max(extract_date) as extract_date,
        dateadd('day', 1, max(extract_date)) as observed_date
    from {{ ref('stg_emis_list_size') }}
),

basis as (
    select
        emis_extract.extract_date,
        emis_extract.observed_date,
        coalesce(min(recorded.dbt_valid_from)::date <= emis_extract.observed_date, false) as use_snapshot
    from emis_extract
    left join {{ ref('int_patient_registrations_current_snapshot') }} as recorded
        on true
    group by emis_extract.extract_date, emis_extract.observed_date
),

as_recorded as (
    select
        recorded.practice_ods_code,
        count(distinct recorded.person_id) as registered_patients
    from {{ ref('int_patient_registrations_current_snapshot') }} as recorded
    inner join basis
        on basis.use_snapshot
        and recorded.dbt_valid_from::date <= basis.observed_date
        and (
            recorded.dbt_valid_to is null
            or recorded.dbt_valid_to::date > basis.observed_date
        )
    group by recorded.practice_ods_code
),

rebuilt as (
    -- Fallback for extracts older than the snapshot: rebuilt from current
    -- history, which loses registrations that EMIS has since redated.
    select
        registrations.practice_ods_code,
        count(distinct registrations.person_id) as registered_patients
    from {{ ref('int_patient_registrations') }} as registrations
    inner join basis
        on not basis.use_snapshot
        and registrations.registration_start_date <= basis.extract_date
        and (
            registrations.effective_end_date is null
            or registrations.effective_end_date >= basis.extract_date
        )
    group by registrations.practice_ods_code
)

select
    counts.practice_ods_code,
    counts.registered_patients as regular_registered_patients,
    iff(basis.use_snapshot, 'As recorded', 'Rebuilt') as olids_count_basis,
    basis.extract_date as snapshot_date
from (
    select practice_ods_code, registered_patients from as_recorded
    union all
    select practice_ods_code, registered_patients from rebuilt
) as counts
cross join basis
