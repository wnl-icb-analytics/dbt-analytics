-- Guards the one piece of logic the diabetes care-processes decomposition duplicates.
-- fct_person_diabetes_9_care_processes_snapshot_input carries foot_check_qualifies -- the
-- date-independent part of fct_person_diabetes_8_care_processes' foot_check_completed_in_last_12m
-- (an adequate exam of both feet, not declined/unsuitable) -- so cltcs_monthly_covariates can
-- re-apply the 12-month window as-at each index_date. If the fct's foot rule changes and the
-- thin input isn't updated, the two silently drift and the reconstructed care_processes_completed
-- diverges from the source (and treated-arm) definition.
--
-- For anyone whose latest foot check is within the last 12 months, the fct's
-- foot_check_completed_in_last_12m IS its date-independent qualifier, so it must equal the thin
-- input's foot_check_qualifies. The test runs at/after the models are built, so its recency
-- filter is a subset of the fct's build-time recency -- no current_date() boundary flakiness.
--
-- Returns one row per person where the qualifier disagrees with the fct.

with fct as (
    select
        person_id,
        latest_foot_check_date,
        foot_check_completed_in_last_12m
    from {{ ref('fct_person_diabetes_9_care_processes') }}
),

thin as (
    select
        person_id,
        foot_check_qualifies
    from {{ ref('fct_person_diabetes_9_care_processes_snapshot_input') }}
)

select
    fct.person_id,
    fct.latest_foot_check_date,
    fct.foot_check_completed_in_last_12m,
    thin.foot_check_qualifies
from fct
join thin using (person_id)
where fct.latest_foot_check_date >= dateadd(month, -12, current_date())
  and fct.foot_check_completed_in_last_12m <> thin.foot_check_qualifies
