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
Monthly append-only capture of rolling-12-month person activity.

This is an incremental, APPEND-only model: one row per patient per month,
stamped with snapshot_month, so the as-at activity picture can be reconstructed for
control-cohort matching. This history cannot be rebuilt after the fact -- rolling
counts are not retained at source, so capture starts from the first run forward.

Behaviour:
- First run: table is created and the current month is seeded (the is_incremental
  guard is skipped because {{ this }} does not yet exist).
- Re-runs within the same month: the guard makes it a no-op (no double-insert), so
  it is safe to leave in the daily build.
- First run of a new month: appends one fresh row per patient for that month.
- append strategy = pure INSERT: existing history is never rewritten.

Population: any patient present in at least one activity source.
Grain: one row per (sk_patient_id, snapshot_month).

Retention: a post_hook keeps ~4 years (48 months) of monthly captures; older
snapshot_months are deleted after each build (a cheap no-op until a month ages out).

Scheduling: left in the normal (daily) build -- the is_incremental guard
makes daily runs a no-op except the first build of each month.
*/

with patient_spine as (
    select sk_patient_id from {{ ref('fct_person_sus_uec_recent') }}
    union
    select sk_patient_id from {{ ref('fct_person_sus_apc_recent') }}
    union
    select sk_patient_id from {{ ref('fct_person_sus_op_recent') }}
    union
    select sk_patient_id from {{ ref('fct_person_gp_recent') }}
    union
    select sk_patient_id from {{ ref('fct_person_wl_current_count_total') }}
),

captured as (
    select
        date_trunc('month', current_date())::date as snapshot_month,
        current_timestamp()::timestamp_ntz        as captured_at,
        sp.sk_patient_id,

        -- A&E (fct_person_sus_uec_recent)
        zeroifnull(aea.ae_tot_12mo) as ae_tot_12mo,
        zeroifnull(aea.ae_t1_12mo)  as ae_t1_12mo,
        zeroifnull(aea.ae_inj_12mo) as ae_inj_12mo,

        -- Inpatient / APC (fct_person_sus_apc_recent)
        zeroifnull(apca.apc_12mo)     as apc_12mo,
        zeroifnull(apca.apc_nel_12mo) as apc_nel_12mo,
        zeroifnull(apca.apc_los_12mo) as apc_los_12mo,
        zeroifnull(apca.acs_nel_12mo) as acs_nel_12mo,

        -- Outpatient (fct_person_sus_op_recent)
        zeroifnull(opa.op_att_tot_12mo) as op_att_tot_12mo,
        zeroifnull(opa.op_spec_12mo)    as op_spec_12mo,
        zeroifnull(opa.op_prov_12mo)    as op_prov_12mo,

        -- GP (fct_person_gp_recent)
        zeroifnull(gpa.gp_att_tot_12mo) as gp_att_tot_12mo,
        zeroifnull(gpa.gp_app_tot_12mo) as gp_app_tot_12mo,
        zeroifnull(gpa.gp_dna_tot_12mo) as gp_dna_tot_12mo,

        -- Waiting list (fct_person_wl_current_count_total)
        zeroifnull(wl.wl_current_total_count)              as wl_current_total_count,
        zeroifnull(wl.wl_current_distinct_providers_count) as wl_current_distinct_providers_count,
        zeroifnull(wl.wl_current_distinct_tfc_count)       as wl_current_distinct_tfc_count,
        coalesce(wl.same_tfc_multiple_providers_flag, false)        as same_tfc_multiple_providers_flag,
        coalesce(wl.current_waiting_list_arrays, array_construct()) as current_waiting_list_arrays

    from patient_spine sp
    left join {{ ref('fct_person_sus_uec_recent') }} aea
        on sp.sk_patient_id = aea.sk_patient_id
    left join {{ ref('fct_person_sus_apc_recent') }} apca
        on sp.sk_patient_id = apca.sk_patient_id
    left join {{ ref('fct_person_sus_op_recent') }} opa
        on sp.sk_patient_id = opa.sk_patient_id
    left join {{ ref('fct_person_gp_recent') }} gpa
        on sp.sk_patient_id = gpa.sk_patient_id
    left join {{ ref('fct_person_wl_current_count_total') }} wl
        on sp.sk_patient_id = wl.sk_patient_id
)

select * from captured
{% if is_incremental() %}
-- Idempotency guard: skip if this calendar month is already captured, so re-running
-- the daily build within the month does not double-insert. On the first ever run the
-- table does not exist, this block is skipped, and the current month is seeded.
where snapshot_month not in (select distinct snapshot_month from {{ this }})
{% endif %}
