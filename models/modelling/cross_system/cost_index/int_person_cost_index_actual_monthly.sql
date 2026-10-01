-- Per-person patient-attributable actual spend by month. SLAM acute only.
-- EPD prescribing is held OUT of the resource-index actual while the feed is
-- stale (last complete month 2025-07 = only ~3 of 12 window months). It
-- survives as a tracked, coverage-flagged source in the wider cost index
-- (fct_person_cost_index_monthly / fct_cost_index_by_org_month); re-add the
-- union here once the feed matures.
{{ config(materialized = 'table') }}

select
    sk_patient_id,
    activity_month,
    sum(coalesce(total_cost, 0)) as actual_cost
from {{ ref('int_cost_index_slam_activity_monthly') }}
where is_patient_attributable
  and sk_patient_id is not null
  and cost_basis = 'actual'
group by 1, 2
