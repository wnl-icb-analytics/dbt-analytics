{{
    config(
        materialized='view',
        tags=['intermediate', 'appointment', 'gp']
    )
}}

/*
Recent-window view over int_appointment_gp_clean.

Grain: same as int_appointment_gp_clean (one row per appointment).

Scope: rolling last 60 months of appointment data, anchored to the
latest start_date in int_appointment_gp_clean. Data-driven (not
CURRENT_DATE) so the window stays stable when data refresh is delayed.

Why this view exists:
OLIDS only retains records for patients who left or died within the
last ~5 years. Anything older than that has been silently truncated to
currently-registered patients only. Reporting beyond 5 years therefore
systematically under-represents the leavers cohort and biases every
metric — per-capita cost goes down, DNA rates drift, workforce mix
tilts. Restricting reporting to a 5-year window matching OLIDS
retention keeps the population denominator honest.

All reporting models in models/reporting/olids/gp_access/ and
models/reporting/olids/gp_costs/ build from this view rather than
the unfiltered int_appointment_gp_clean. The semantic view
sem_olids_appointments also references this view.

The unfiltered int_appointment_gp_clean remains available for ad-hoc
analysts who explicitly need pre-5-year history (with the OLIDS
retention caveat in mind).
*/

with recent_window as (
    select
        DATEADD('month', -59, DATE_TRUNC('month', MAX(start_date))) as min_start_month
    from {{ ref('int_appointment_gp_clinical') }}
    where start_date is not null
)

select
    a.*
from {{ ref('int_appointment_gp_clinical') }} as a
cross join recent_window as w
where DATE_TRUNC('month', a.start_date) >= w.min_start_month
