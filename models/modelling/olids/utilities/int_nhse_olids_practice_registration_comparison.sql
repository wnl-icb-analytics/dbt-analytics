{{
    config(
        materialized='table',
        tags=['data_quality', 'utilities', 'nhse', 'olids']
    )
}}

with snapshots as (
    -- OLIDS keeps Left and Deceased registrations for five years, so only
    -- snapshots within the last 60 months still hold everyone registered then.
    select distinct snapshot_date
    from {{ ref('practice_registered_patients') }}
    where snapshot_date >= dateadd('month', -60, current_date)
),

olids_registered as (
    -- One practice per person on each date, matching the NHSE whole-person
    -- headcount. The registration with the latest start wins, as in
    -- dim_person_active_patients; the episode id settles exact ties.
    select
        snapshots.snapshot_date,
        registrations.practice_ods_code as practice_code,
        registrations.person_id
    from snapshots
    inner join {{ ref('int_patient_registrations') }} as registrations
        on registrations.registration_start_date <= snapshots.snapshot_date
        and (
            registrations.effective_end_date is null
            or registrations.effective_end_date >= snapshots.snapshot_date
        )
    qualify row_number() over (
        partition by snapshots.snapshot_date, registrations.person_id
        order by
            registrations.registration_start_date desc,
            registrations.patient_id desc,
            registrations.registration_record_id desc
    ) = 1
),

olids_counts as (
    select
        snapshot_date,
        practice_code,
        count(*) as olids_registered_patients
    from olids_registered
    group by snapshot_date, practice_code
),

olids_practices as (
    select distinct practice_code from olids_counts
),

nhse_counts as (
    -- NHSE covers every practice in England; keep practices that send data to OLIDS.
    select
        nhse.snapshot_date,
        nhse.practice_code,
        sum(nhse.registered_patients) as nhse_registered_patients
    from {{ ref('practice_registered_patients') }} as nhse
    inner join snapshots
        on nhse.snapshot_date = snapshots.snapshot_date
    inner join olids_practices
        on nhse.practice_code = olids_practices.practice_code
    group by nhse.snapshot_date, nhse.practice_code
),

comparison as (
    select
        coalesce(nhse.snapshot_date, olids.snapshot_date) as snapshot_date,
        coalesce(nhse.practice_code, olids.practice_code) as practice_code,
        nhse.nhse_registered_patients,
        coalesce(olids.olids_registered_patients, 0) as olids_registered_patients
    from nhse_counts as nhse
    full outer join olids_counts as olids
        on nhse.snapshot_date = olids.snapshot_date
        and nhse.practice_code = olids.practice_code
),

measured as (
    select
        comparison.snapshot_date,
        comparison.practice_code,
        practices.practice_name,
        practices.borough_registered as borough,
        comparison.nhse_registered_patients,
        comparison.olids_registered_patients,
        comparison.olids_registered_patients - comparison.nhse_registered_patients as difference,
        abs(comparison.olids_registered_patients - comparison.nhse_registered_patients) as absolute_difference,
        round(
            (comparison.olids_registered_patients - comparison.nhse_registered_patients) * 100.0
            / nullif(comparison.nhse_registered_patients, 0),
            2
        ) as percent_difference
    from comparison
    left join {{ ref('dim_practice') }} as practices
        on comparison.practice_code = practices.practice_code
)

select
    snapshot_date,
    practice_code,
    practice_name,
    borough,
    nhse_registered_patients,
    olids_registered_patients,
    difference,
    absolute_difference,
    percent_difference,
    abs(percent_difference) as absolute_percent_difference,
    snapshot_date = max(snapshot_date) over () as is_latest_snapshot
from measured
