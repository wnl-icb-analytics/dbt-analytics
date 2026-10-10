-- LTC LCS: Model of Care activity - Care plan sharing completed
-- Flags persons with a Long term condition summary sent to patient event in the last 12 months.
-- Used by HRCS2B / HRS2B searches.

with events as (
    select
        person_id,
        clinical_effective_date
    -- careplan_sharing_completed_vs1 by GUID: the friendly name also matches two
    -- "Chronic disease initial assessment" value sets from other reports
    from ({{ get_ltc_lcs_observations_latest("53bb7013-ac13-56dd-ec35-5425a5e6f2f7,1e4b1d73-eaed-7e8d-da5e-0a77ca874c17") }})
    where clinical_effective_date >= dateadd(month, -12, current_date())
      and clinical_effective_date <= current_date()
)

select
    person_id,
    max(clinical_effective_date) as latest_completed_date,
    true as careplan_sharing_completed
from events
group by person_id
