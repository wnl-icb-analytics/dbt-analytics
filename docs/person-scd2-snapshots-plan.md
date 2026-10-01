# Plan: point-in-time history for person-level measures (SCD2 + monthly capture)

**Status:** Partly implemented. The C-LTCS score snapshots (§4.1), the seven
consolidated-table snapshots (§4.2), and the monthly append capture (§6.1) are built
and compile-validated. Decisions actioned: scheduling **Option A** (left in the daily
build), and a **4-year (48-month) retention prune** via `post_hook` on
`cltcs_activity_monthly_capture`. Backfill: none — history starts from first run.
**Author:** (drafted for E. Baldwin)
**Date:** 2026-07-08 (supersedes 2026-06-24 draft)

## 1. Goal

Track how person-level measures move over time, for two uses:

1. **Control-cohort matching** — compare a C-LTCS patient's score/covariate
   state *as-at a date* against candidate controls with the same state at the
   same date.
2. **Trajectory analysis** — see when a person's conditions, complexity,
   frailty, medication burden or activity change.

Two mechanisms, chosen by how the source behaves:

- **SCD2 snapshots** (`dbt snapshot`, check strategy) for *stable* state that
  changes occasionally — a new version only when a tracked value actually
  changes. Compact.
- **Monthly append captures** (incremental, append-only) for *volatile* rolling
  metrics (12-month activity counts) that change on every build — one row per
  patient per month, so SCD2 would explode (see §6).

## 2. Conventions confirmed in this repo

Follow the actual code, not the (stale) `docs/archive/snapshots-guide.md`.

- **Format:** Snapshots are **YAML** files in [`snapshots/`](../snapshots), one
  `*_snapshot.yml` per source relation. Examples:
  [`cltcs_cohort_membership_snapshot.yml`](../snapshots/cltcs_cohort_membership_snapshot.yml),
  [`cltcs_score_treatment_snapshot.yml`](../snapshots/cltcs_score_treatment_snapshot.yml).
- **House config block:**
  ```yaml
  config:
    strategy: check
    unique_key: [person_id]          # or [patient_id] / [sk_patient_id]
    updated_at: table_refresh_date    # optional; stamps dbt_valid_from/to
    hard_deletes: invalidate          # close dbt_valid_to when a row leaves source
    check_cols: [ ... ]               # selective list, or `all`
  ```
- **`relation`** is a `ref(...)` (or `source(...)`) — snapshots cannot project
  or filter columns. To snapshot a **subset** of a wide model, build a thin
  input view first and point the snapshot at that (the pattern used by
  [`cltcs_cohort_membership`](../models/published/direct_care/C_LTCS/cltcs_cohort_membership.sql)
  and the two score snapshot inputs).
- **Storage** is central in [`dbt_project.yml`](../dbt_project.yml) under
  `snapshots: wnl_analytics:` — `database: MODELLING`,
  `target_schema: DBT_SNAPSHOTS`, plus an `add_model_comment()` post-hook. New
  snapshots inherit this; **no per-file schema/database config is needed.**
- **`table_refresh_date`** is the convention for the optional `updated_at`
  column under the check strategy — added in the input view as
  `current_timestamp()::timestamp_ntz as table_refresh_date`. It is **not** a
  `check_col` (it changes every run, so it would version every row).
- **Tests:** snapshots carry `columns:` with `not_null` on the unique key and
  on `dbt_valid_from`.

## 3. What already exists (no duplication)

- `cltcs_cohort_membership_snapshot` — C-LTCS cohort entry/exit + processing
  `area_code`.
- `fct_person_ltc_lcs_risk_summary_snapshot` — LTC-LCS risk groups.
- `fct_person_myria_control_group_published_snapshot` /
  `..._high_risk_patients_published_snapshot` — Myria commissioning cohorts.

None cover the score subdomains or the consolidated clinical tables below.

## 4. SCD2 snapshot targets

### 4.1 C-LTCS scores — IMPLEMENTED in this change

Support control-cohort matching on the score components. Each score model is
wide (raw / clipped / 0-100 intermediates), so a **thin input view** projects
only the identifier, the scaled subdomain scores and the final score, plus
`table_refresh_date`:

| Snapshot | Input view | Contents |
|----------|-----------|----------|
| [`cltcs_score_treatment_snapshot`](../snapshots/cltcs_score_treatment_snapshot.yml) | [`cltcs_score_treatment_snapshot_input`](../models/reporting/cltcs/cltcs_score_treatment_snapshot_input.sql) | `sk_patient_id`, 6 `scaled_score_*`, `score_treatment` |
| [`cltcs_score_frailty_snapshot`](../snapshots/cltcs_score_frailty_snapshot.yml) | [`cltcs_score_frailty_snapshot_input`](../models/reporting/cltcs/cltcs_score_frailty_snapshot_input.sql) | `sk_patient_id`, 7 `scaled_score_*`, `score_exclusions`, `score_frailty` |

- `unique_key: [sk_patient_id]`, `hard_deletes: invalidate`,
  `updated_at: table_refresh_date`.
- **`check_cols` is the final score only** (`score_treatment` / `score_frailty`).
  The scaled subdomain scores ride along — captured as-at each version open, but
  do not themselves trigger versions. See §5 for why.
- The input views alias the cohort key `patient_id` (the commissioning layer's
  pseudonymised patient key, = `sk_patient_id`) as `sk_patient_id`. If we later
  prefer consistency with `cltcs_cohort_membership_snapshot` (which keys on
  `patient_id`), this is a one-line change in both places.

> These snapshots depend on the four `cltcs_score_*` models. The models were
> intentionally moved to score a larger cohort off `dim_person_demographics_basic`
> (keyed on `sk_patient_id`) rather than the C-LTCS-only `cltcs_patient_list`.
> That switch left them non-compiling because the OLIDS joins still used
> `olids_id`/`patient_id` and the z-scores still `partition by area_code`, none of
> which exist on the new source. Fixed in this change by a shared
> **`cltcs_adult_population`** model (same folder) that defines the living-adult
> population once (`dim_person_demographics_basic` + `dim_person_pseudo` for
> `person_id` via a latest-registered-record tiebreak, plus `neighbourhood_code`
> from `stg_cltcs_emis_cltcs_local_mapping_nh_gp`), which all four score models now
> `select * from` instead of repeating. The identity resolution is a documented
> stopgap (see the model header): it should move to a central bridge and does not
> handle fragmented records well. Plus, within the score models:
> - switching the OLIDS joins `olids_id` → `person_id` and the activity joins
>   `patient_id` → `sk_patient_id` (using the `person_id` from the shared model);
> - scaling by **`neighbourhood_code`** (from
>   `stg_cltcs_emis_cltcs_local_mapping_nh_gp`, joined on `practice_code`) in
>   place of `area_code` — `neighbourhood_code` is threaded through
>   `domain_sub_scores`/`composite_scores` and used in every `partition by`.
>   It is null for practices outside the Haringey/Camden local mapping, so those
>   patients scale together in the null group.
> The models output `sk_patient_id`; `cltcs_scores` was aligned to select
> `sk_patient_id` from them.

### 4.2 Consolidated clinical tables — IMPLEMENTED in this change

These are the high-value matching covariates that feed
[`cltcs_cohort_data`](../models/published/direct_care/C_LTCS/cltcs_cohort_data.sql).
All are stable per-person state → good SCD2 fit. All keyed `unique_key: [person_id]`,
`hard_deletes: invalidate`, no `updated_at` (dbt stamps validity with the run time).

Built (in `snapshots/`): `dim_person_conditions_snapshot`,
`fct_person_behavioural_risk_factors_snapshot`, `dim_person_ccms_snapshot`,
`int_person_frailty_snapshot`, `dim_person_care_home_snapshot`,
`fct_person_polypharmacy_current_snapshot`, `dim_person_demographics_snapshot`.
New supporting models: thin projections `dim_person_ccms_snapshot_input` and
`dim_person_demographics_snapshot_input` (excludes `age`), and the consolidated
`int_person_frailty` view (eFI2 + Rockwood + frailty register, one row per person).

| Target | Why | `check_cols` / notes |
|--------|-----|----------------------|
| `dim_person_conditions` | Core comorbidity / disease-register state — strongest matching covariates | `all` — auto-tracks new condition flags; no volatile columns |
| `fct_person_behavioural_risk_factors` | Smoking / BMI / alcohol risk | `all` — all deterministic |
| `dim_person_ccms` | Cambridge multimorbidity score — single complexity covariate | **thin projection** required: `last_updated` and `ccms_score_id` change every run ([`int_ccms_score.sql:49`](../models/modelling/olids/risk_stratification/int_ccms_score.sql)); snapshot a view exposing `person_id`, `cambridge_comorbidity_score` only, `check_cols: [cambridge_comorbidity_score]` |
| Consolidated **frailty** view (new) | Frailty trajectory is central to matching and to `score_frailty`; today it is three feeders — `stg_aic_int_efi2_scores`, `int_rockwood_latest`, `fct_person_frailty_register` | build one `int_person_frailty` view (one row/person: eFI2 score+band, Rockwood score+band, latest frailty severity), then snapshot with `check_cols` on the scores/bands |
| `dim_person_care_home` | Care-home / residence entry is a major state change and a key confounder | `check_cols` on `is_care_home_resident`, `residence_type`, `residence_status` |
| `fct_person_polypharmacy_current` | Medication burden over time | `check_cols` on `medication_count`, `is_polypharmacy_5plus`, `is_polypharmacy_10plus` (not the medication_name_list array) |
| `dim_person_demographics` — **structural fields only** | Match keys: sex, ethnicity, main language, registered practice, death | thin view of stable fields only; **exclude `age`** (changes continuously → would version every birthday-adjacent run) |

## 5. `check_cols` and volatile-column discipline

The single most important design rule: **never put an every-run column in
`check_cols`**, or every row versions on every snapshot.

- **Cohort-relative scores** (the C-LTCS scores) are z-scored per `neighbourhood_code`
  (treatment also age-adjusted), so they can drift slightly on every build even
  when a patient's raw features don't change. We therefore key versioning on the
  **final score only**, rounded to 1dp — sub-0.1 drift does not version, which
  limits churn, and the scaled components are still captured whenever the score
  moves. (Chosen over `check_cols` on every scaled score, which would version on
  every build.)
- **`dim_person_ccms`** — `last_updated`/`ccms_score_id` change every run, so
  `check_cols: all` is unsafe; use a thin projection (§4.2). *Optionally*, make
  `int_ccms_score` deterministic (derive `last_updated` from source dates, build
  the surrogate key from `[person_id, cambridge_comorbidity_score]`) so
  `check_cols: all` becomes safe — larger change, out of scope here.
- **`dim_person_conditions` / `fct_person_behavioural_risk_factors`** — no
  volatile columns, so `check_cols: all` is both safe and resilient (new flags
  tracked automatically).
- **Validation check:** run `dbt snapshot` twice with no source change; the
  second run must add **zero** rows. This is the definitive test that
  `check_cols` is right.

## 6. Volatile activity tables → monthly append capture

The rolling-12-month activity models are **not** SCD2 candidates — their counts
change on essentially every build, so a check-strategy snapshot would create a
new version for nearly every patient every run. Instead capture them **once per
month, append-only**, giving one row per patient per month.

Tables in scope (all keyed on `sk_patient_id`):
`fct_person_sus_uec_recent`, `fct_person_sus_apc_recent`, `fct_person_sus_op_recent`,
`fct_person_gp_recent`, `fct_person_medications_recent`,
`fct_person_wl_current_count_total`.

Prefer **one wide monthly capture** joining these per patient, rather than one
model per source — simpler to schedule, query and retain.

### 6.1 Recommended: append-only incremental model — IMPLEMENTED in this change

dbt has no native "monthly append", so use an **incremental model** with
`incremental_strategy='append'` and a `snapshot_month` stamp, guarded so it
only inserts once per calendar month.

Built as
[`cltcs_activity_monthly_capture`](../models/reporting/cltcs/cltcs_activity_monthly_capture.sql)
(+ `.yml` with a `dbt_utils.unique_combination_of_columns` test on
`[sk_patient_id, snapshot_month]`). It captures A&E / inpatient / outpatient / GP /
medications / waiting-list rolling-12-month counts over a union spine of all six
sources (any patient with activity in at least one), `zeroifnull`-ed. Sketch:

`models/reporting/cltcs/cltcs_activity_monthly_capture.sql`:
```sql
{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        on_schema_change='append_new_columns',
        cluster_by=['snapshot_month']        -- Snowflake: prune by month
    )
}}

with captured as (
    select
        date_trunc('month', current_date())::date as snapshot_month,
        current_timestamp()::timestamp_ntz        as captured_at,
        ae.sk_patient_id,
        ae.ae_tot_12mo,
        ae.ae_inj_12mo,
        ip.apc_nel_12mo,
        ip.apc_los_12mo,
        gp.gp_att_tot_12mo,
        rm.unique_active_ingredient_count_12mo,
        wl.wl_current_total_count
        -- ...add the columns you want history for
    from {{ ref('fct_person_sus_uec_recent') }} ae
    left join {{ ref('fct_person_sus_apc_recent') }} ip using (sk_patient_id)
    left join {{ ref('fct_person_gp_recent') }}     gp using (sk_patient_id)
    left join {{ ref('fct_person_medications_recent') }} rm using (sk_patient_id)
    left join {{ ref('fct_person_wl_current_count_total') }} wl using (sk_patient_id)
)

select * from captured
{% if is_incremental() %}
-- idempotency guard: skip if this month is already captured, so re-running the
-- daily pipeline within the month does not double-insert. On the very first
-- run the table does not exist, this block is skipped, and the current month
-- is seeded.
where snapshot_month not in (select distinct snapshot_month from {{ this }})
{% endif %}
```

How it behaves:
- **First run:** table created, current month seeded.
- **Subsequent runs same month:** `where` guard returns no rows → nothing
  appended (safe to leave in the daily build).
- **First run of a new month:** guard passes → one fresh row per patient
  appended, stamped with the new `snapshot_month`.
- **`append`** = pure `INSERT` (no merge/update): existing history is never
  rewritten, so it is cheap and immutable.

### 6.2 Scheduling

Two ways to get monthly cadence:

- **A (simplest): leave it in the daily run.** The `is_incremental()` guard
  makes it a no-op except on the first build of each month, so daily scheduling
  produces monthly data with no separate job. Recommended.
- **B: separate monthly invocation.** Tag the model
  (`config(tags=['monthly_capture'])`), exclude the tag from the daily run, and
  run `dbt run -s tag:monthly_capture` from a monthly scheduled job. Use this
  only if you want it fully decoupled from the daily build.

Either way, ensure the upstream `fct_person_*_recent` models are built before the
capture runs so it sees fresh counts.

### 6.3 Grain, tests, retention

- **Grain:** one row per `(sk_patient_id, snapshot_month)`. Test with
  `dbt_utils.unique_combination_of_columns` on those two, plus `not_null` on
  both.
- **Querying "as-at":** filter `where snapshot_month = '<month>'`, or for the
  state on any date use
  `qualify row_number() over (partition by sk_patient_id order by snapshot_month desc)`
  over months `<=` the target.
- **Retention:** append grows unbounded, so a `post_hook` on the model prunes to
  the last **48 months (4 years)**:
  `delete from {{ this }} where snapshot_month < dateadd(month, -48, date_trunc('month', current_date()))`.
  It runs after each build (a cheap no-op until a month ages out) and is kept
  separate from the append SELECT so it can't corrupt the capture logic.
- **No backfill:** append-only captures history from first run forward; it
  cannot reconstruct months before it existed. If pre-launch history is needed,
  that is a one-off backfill from any existing archive, out of scope here.

### 6.4 Alternative considered — timestamp-strategy snapshot

A `strategy: timestamp` snapshot with `updated_at = date_trunc('month', current_date())`
also yields one version per patient per month (a new version opens only when the
month rolls). It reuses the snapshot machinery and gives SCD2 validity windows,
but produces SCD2 rows rather than a clean append fact and is less transparent
to query for "as-at". Use the incremental append (§6.1) unless SCD2 windows on
the activity data are specifically wanted.

## 7. Testing, running & scheduling (SCD2 snapshots)

- **Build sources first:** `dbt build -s <source models>`.
- **First snapshot run** (baseline, all `dbt_valid_to = null`):
  `dbt snapshot -s <snapshot names>`.
- **Validate:** `dbt test -s <snapshot>` (the `not_null` tests), and the
  zero-new-rows re-run check from §5.
- **Schedule:** snapshots run **daily before the main build** in production;
  new snapshots inherit that cadence once merged. Confirm the source models are
  built upstream of the daily `dbt snapshot` step.

## 8. Follow-ups (out of scope here)

- Refresh [`docs/archive/snapshots-guide.md`](archive/snapshots-guide.md) to the
  current YAML style and retire the legacy `{% snapshot %}` examples.
- Build the §4.2 snapshots and the §6 monthly capture as their own reviewed
  changes with tests.
- If chosen, the `int_ccms_score` determinism fix (§5) as its own change.
