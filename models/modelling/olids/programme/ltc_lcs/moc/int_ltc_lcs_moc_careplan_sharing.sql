-- LTC LCS: Model of Care activity - Care plan sharing completed
-- Flags persons with a Long term condition summary sent to patient event in the last 12 months.
-- Used by HRCS2B / HRS2B searches.

with events as (
    select
        person_id,
        clinical_effective_date
    from ({{ get_ltc_lcs_observations_latest("careplan_sharing_completed_vs1") }})
    where clinical_effective_date >= dateadd(month, -12, current_date())
      and clinical_effective_date <= current_date()
)

select
    person_id,
    max(clinical_effective_date) as latest_completed_date,
    true as careplan_sharing_completed
from events
group by person_id
