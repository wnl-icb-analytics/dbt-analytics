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
pool that month; reappearing is a re-entry. Identity + geography are carried (person_id,
neighbourhood_code). Most covariates are read as-at elsewhere from the SCD2 snapshots; the
exception is the LTC LCS Model of Care (MoC) stage and status, captured here per month from
fct_person_ltc_lcs_risk_summary. The risk-summary snapshot only versions on risk-group
changes, so MoC progress between those changes is not recorded there.

MoC columns were added after the capture began: rows captured before then hold NULL for all
three, including is_in_ltc_lcs_moc_base. From then on, is_in_ltc_lcs_moc_base is false and the
MoC columns NULL for patients not on the LTC LCS MoC base population.

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
        p.sk_patient_id,
        p.person_id,
        p.neighbourhood_code,

        -- LTC LCS Model of Care, as-at the captured month
        rs.person_id is not null as is_in_ltc_lcs_moc_base,
        rs.moc_stage_completed_label,
        rs.moc_pathway_status
    from {{ ref('cltcs_adult_population') }} p
    left join {{ ref('fct_person_ltc_lcs_risk_summary') }} rs
        on rs.person_id = p.person_id
    where p.sk_patient_id is not null and p.sk_patient_id <> '1'
)

select * from captured
{% if is_incremental() %}
-- Idempotency guard: skip if this calendar month is already captured, so re-running the
-- daily build within the month does not double-insert. On the first ever run the table does
-- not exist, this block is skipped, and the current month is seeded.
where snapshot_month not in (select distinct snapshot_month from {{ this }})
{% endif %}
