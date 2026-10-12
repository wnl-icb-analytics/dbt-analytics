{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        on_schema_change='append_new_columns',
        cluster_by=['snapshot_month'],
        tags=['cltcs', 'monthly_capture'],
        post_hook="delete from {{ this }} where snapshot_month < dateadd(month, -48, date_trunc('month', current_date()))"
    )
}}

/*
Monthly append-only capture of the C-LTCS control-candidate population roster.

cltcs_adult_population is current-state (living complex adults registered now), so its
membership changes over time as patients age in, die, register/deregister or cross the
complexity/activity thresholds. To know who was in the pool as-at a past month, capture the
roster once per calendar month, append-only: one row per patient per month they were in the
population. Same mechanism as cltcs_activity_monthly_capture.

Behaviour:
- First run: the table is created and the current month is seeded (the is_incremental guard
  is skipped because {{ this }} does not yet exist).
- Re-runs within the same month: the guard makes it a no-op (no double-insert), so it is
  safe to leave in the daily build.
- First run of a new month: appends the current roster stamped with the new snapshot_month.
- append strategy = pure INSERT: existing history is never rewritten.

Membership is captured, not derived: a patient absent from a later month's roster left the
pool that month; reappearing is a re-entry. Only stable identity + geography are carried
(person_id, neighbourhood_code); covariates are read as-at elsewhere from the SCD2 snapshots,
never from here.

Population: cltcs_adult_population (sentinel sk_patient_ids excluded).
Grain: one row per (sk_patient_id, snapshot_month).

Retention: a post_hook keeps ~4 years (48 months) of monthly rosters; older snapshot_months
are deleted after each build (a cheap no-op until a month ages out).

Scheduling: left in the normal (daily) build -- the is_incremental guard makes daily runs a
no-op except the first build of each month.
*/

with captured as (
    select
        date_trunc('month', current_date())::date as snapshot_month,
        current_timestamp()::timestamp_ntz        as captured_at,
        sk_patient_id,
        person_id,
        neighbourhood_code
    from {{ ref('cltcs_adult_population') }}
    where sk_patient_id is not null and sk_patient_id <> '1'
)

select * from captured
{% if is_incremental() %}
-- Idempotency guard: skip if this calendar month is already captured, so re-running the
-- daily build within the month does not double-insert. On the first ever run the table does
-- not exist, this block is skipped, and the current month is seeded.
where snapshot_month not in (select distinct snapshot_month from {{ this }})
{% endif %}
