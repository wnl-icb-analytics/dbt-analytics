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
Monthly append-only capture of recent-medication covariates.

fct_person_medications_recent is a rolling "recent" aggregate: one row per person built from
each person's medication orders in the last 30 days and last year (both windows anchored on
CURRENT_DATE). Like the SUS/GP/WL activity metrics and the ASC capture, that picture changes
with the clock and cannot be reconstructed historically, so it is captured append-only per
month rather than snapshotted (its array_agg medications_recent_* arrays would also churn a
check-strategy snapshot). Keyed on person_id (not sk_patient_id): a person can have
prescriptions but no recent SUS/GP/WL activity, so this is its own capture rather than being
folded into cltcs_activity_monthly_capture. One row per (person_id, snapshot_month).

Behaviour mirrors cltcs_asc_monthly_capture:
- First run: table is created and the current month is seeded (the is_incremental guard is
  skipped because {{ this }} does not yet exist).
- Re-runs within the same month: the guard makes it a no-op (safe in the daily build).
- First run of a new month: appends one fresh row per person for that month.
- append strategy = pure INSERT: existing history is never rewritten.

Population: any person in fct_person_medications_recent (i.e. with a prescription in the last
30 days or last year).
Grain: one row per (person_id, snapshot_month).
Retention: a post_hook keeps ~4 years (48 months) of monthly captures.
Forward-only: history starts at the first run; it cannot backfill months before it existed.

WARNING (operational): never run this model with --full-refresh. The append-only history is not
reconstructable, so a full refresh drops the table and re-seeds ONLY the current month, permanently
deleting every previously captured month. The same hazard applies to the sibling
cltcs_*_monthly_capture models.

Only the two 12-month columns (medications_recent_12mo, unique_active_ingredient_count_12mo)
are consumed by cltcs_monthly_covariates; the 30-day columns and sibling 12-month counts are
captured for future use.
*/

with captured as (
    select
        date_trunc('month', current_date())::date as snapshot_month,
        current_timestamp()::timestamp_ntz        as captured_at,
        person_id,

        -- consumed by cltcs_monthly_covariates
        medications_recent_12mo,
        unique_active_ingredient_count_12mo,

        -- captured for future use (not surfaced in the panel)
        total_prescriptions_12mo,
        unique_medication_count_12mo,
        medications_recent_30d,
        total_prescriptions_30d,
        unique_medication_count_30d,
        unique_active_ingredient_count_30d

    from {{ ref('fct_person_medications_recent') }}
    where person_id is not null
)

select * from captured
{% if is_incremental() %}
-- Idempotency guard: skip if this calendar month is already captured, so re-running the
-- daily build within the month does not double-insert. On the first ever run the table does
-- not exist, this block is skipped, and the current month is seeded.
where snapshot_month not in (select distinct snapshot_month from {{ this }})
{% endif %}
