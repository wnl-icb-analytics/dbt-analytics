/*
Primary care prescribing cost per patient per month, from EPD.

Grain: one row per sk_patient_id per activity_month (first of the processing
period month). Latest submission only (the feed restates each period as a full
reload — see stg_epd_pc_meds.is_latest_submission), so cost is not double
counted.

Cost: item_actual_cost is the actual reimbursed spend in pence; divided by 100
to £ to align with the SUS tariff (£) and MH proxy-cost (£) PODs that feed the
person cost model.
Cost basis: 'actual' = EPD item_actual_cost reimbursed spend.

Covers the WNL footprint (the EPD feed was repointed to NCL + NWL 2026-06).
Patients with no pseudo key (sk_patient_id NULL — out-of-area / unmatched) are
dropped, so this joins cleanly to the person dimensions like the other POD
summaries. NOTE: the feed lags — check the epd_covers_slam_cost_window test
before relying on recent months.
*/

select
    sk_patient_id
    , date_trunc('month', processing_period_date)   as activity_month
    , 'actual'                                      as cost_basis
    , round(sum(item_actual_cost) / 100, 2)         as prescribing_cost
    , sum(item_count)                               as prescribing_items
from {{ ref('stg_epd_pc_meds') }}
where is_latest_submission
    and sk_patient_id is not null
group by
    sk_patient_id
    , date_trunc('month', processing_period_date)
