-- LTC LCS: Model of Care review activity
-- Flags Chronic disease management annual review events in the last 12 months.
-- Surfaced as MDT Review for HRCS/HRS and Remote Desktop Review for MRS/LRS.

with events as (
    select
        person_id,
        clinical_effective_date
    from ({{ get_ltc_lcs_observations_latest("mdt_review_completed_vs1") }})
    where clinical_effective_date >= dateadd(month, -12, current_date())
      and clinical_effective_date <= current_date()
)

select
    person_id,
    max(clinical_effective_date) as latest_completed_date,
    true as review_completed
from events
group by person_id
