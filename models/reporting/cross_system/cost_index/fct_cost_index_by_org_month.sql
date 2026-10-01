/*
SLAM commissioned cost by organisation and month — reporting aggregate for
the Aligning Resource Index work.

Grain: activity_month x icb_group x commissioner_code x provider_code x
service_grouping x service. Built from int_cost_index_slam_activity_monthly
(SLAM actual cost spine) — full history from 2021-04 to the last complete
month, no rolling window.

icb_group splits WNL-commissioned (resident) spend from out-of-sector
pass-through (other ICBs' residents treated at WNL providers). Use
in_patch = true for the resource index numerator.

Measures: total_cost / total_activity, plus *_attributable variants
(is_patient_attributable lines only) so per-patient resource cuts exclude
block/adjustment/transport.

Prescribing (EPD) is patient-level with no commissioner/provider — it is not
included here; see fct_person_cost_index_monthly for the patient-level SLAM+EPD
union.
*/

select
    activity_month
    , case
        when commissioner_code in ('W2U3Z', '93C', 'Z9B2Z') then 'WNL in-patch'
        else 'Out-of-sector'
      end                                               as icb_group
    , commissioner_code in ('W2U3Z', '93C', 'Z9B2Z')    as in_patch
    , commissioner_code
    , provider_code
    , service_grouping
    , service
    , sum(total_cost)                                   as total_cost
    , sum(total_activity)                               as total_activity
    , sum(iff(is_patient_attributable, total_cost, 0))  as attributable_cost
    , sum(iff(is_patient_attributable, total_activity, 0)) as attributable_activity
    , count(distinct sk_patient_id)                     as patients
from {{ ref('int_cost_index_slam_activity_monthly') }}
group by
    activity_month
    , icb_group
    , in_patch
    , commissioner_code
    , provider_code
    , service_grouping
    , service
