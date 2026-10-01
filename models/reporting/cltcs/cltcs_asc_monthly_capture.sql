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
Monthly append-only capture of adult social care (ASC) service covariates.

fct_person_asc_service_recent is a rolling "recent" aggregate: one row per patient built
from each local authority's most-recent CLD submission (anchored on CURRENT_DATE, with an
18-month recency window). Like the SUS/GP/WL activity metrics, that picture changes with the
clock and its submission loads and cannot be reconstructed historically, so it is captured
append-only per month rather than snapshotted (its array_agg(distinct ...) arrays would also
churn a check-strategy snapshot). One row per (sk_patient_id, snapshot_month).

Behaviour mirrors cltcs_activity_monthly_capture:
- First run: table is created and the current month is seeded (the is_incremental guard is
  skipped because {{ this }} does not yet exist).
- Re-runs within the same month: the guard makes it a no-op (safe in the daily build).
- First run of a new month: appends one fresh row per patient for that month.
- append strategy = pure INSERT: existing history is never rewritten.

Population: any patient in fct_person_asc_service_recent (i.e. with an ASC service).
Grain: one row per (sk_patient_id, snapshot_month).
Retention: a post_hook keeps ~4 years (48 months) of monthly captures.
Forward-only: history starts at the first run; it cannot backfill months before it existed.
*/

with captured as (
    select
        date_trunc('month', current_date())::date as snapshot_month,
        current_timestamp()::timestamp_ntz        as captured_at,
        sk_patient_id,

        -- presence (1 for anyone with an ASC service)
        has_asc_service,

        -- arrays
        borough_name,
        service_type,
        primary_support_reason_category,

        -- primary-support-reason flags
        has_physical_support_personal_care,
        has_physical_support_access_mobility,
        has_learning_disability_support,
        has_mental_health_support,
        has_unknown_primary_support_reason,
        has_memory_cognition_support,
        has_social_support_unpaid_carer,
        has_social_support_social_isolation,
        has_sensory_support_visual_impairment,
        has_sensory_support_hearing_impairment,
        has_social_support_substance_misuse,
        has_sensory_support_dual_impairment,
        has_social_support_asylum_seeker

    from {{ ref('fct_person_asc_service_recent') }}
    where sk_patient_id is not null and sk_patient_id <> '1'
)

select * from captured
{% if is_incremental() %}
-- Idempotency guard: skip if this calendar month is already captured, so re-running the
-- daily build within the month does not double-insert. On the first ever run the table does
-- not exist, this block is skipped, and the current month is seeded.
where snapshot_month not in (select distinct snapshot_month from {{ this }})
{% endif %}
