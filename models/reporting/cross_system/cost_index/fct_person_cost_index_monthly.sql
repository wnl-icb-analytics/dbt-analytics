/*
Patient-level spend by month and service grouping — canonical cost index for
WNL resource analysis.

Grain: sk_patient_id x activity_month x service_grouping x service x
is_patient_attributable x cost_basis x cost_source x activity_unit.

Full-history union of the cost spines built so far:
  * SLAM (int_cost_index_slam_activity_monthly) — acute / community actual cost, WNL
    (NCL + NWL). From 2021-04 to the last complete month.
  * EPD prescribing (int_cost_index_epd_prescribing_monthly) — GP prescriptions
    (Community), WNL. V1 history followed by the V2 dispensing delivery.
  * MHSDS (int_cost_index_mhsds_activity_monthly) — MH inpatient and contacts.
    Proxy-costed from the NHSE 26/27 currency price schedule (deflated + MFF).
  * CSDS (int_cost_index_csds_activity_monthly) — community contacts.
    Proxy-costed from the NHSE 26/27 currency price schedule (deflated + MFF).

Full history at every layer here — no rolling window. The 12-month analysis
window is applied in reporting facts (fct_person_resource_index).
cost_basis distinguishes actual cost from future proxy/nominal sources.

Still to union (sources identified, build pending): GP appointments (OLIDS)
and high-cost drugs/devices PLD (LSDrPLCM/LSDePLCM).

is_patient_attributable carries through from the POD mapping — filter it for
per-patient resource cuts (excludes SLAM block/adjustment lines);
keep all rows for provider/system totals.

MH/community provider block payments inside SLAM are mostly not
patient-attributable, so patient-attributable cuts do not double count; whole-
system totals mixing SLAM block lines with these proxy sources may double count.
*/

with slam as (
    select
        sk_patient_id
        , activity_month
        , service_grouping
        , service
        , is_patient_attributable
        , cost_basis
        , 'SLAM' as cost_source
        , 'slam_activity' as activity_unit
        , total_cost
        , total_activity
    from {{ ref('int_cost_index_slam_activity_monthly') }}
)


, rx as (
    select
        p.sk_patient_id
        , p.activity_month
        , 'Community'        as service_grouping
        , 'GP Prescriptions' as service
        , true               as is_patient_attributable
        , p.cost_basis
        , 'EPD'              as cost_source
        , 'prescription_item' as activity_unit
        , p.prescribing_cost as total_cost
        , p.prescribing_items as total_activity
    from {{ ref('int_cost_index_epd_prescribing_monthly') }} as p
)

, mhsds as (
    select
        sk_patient_id
        , activity_month
        , 'Mental Health'    as service_grouping
        , service
        , true               as is_patient_attributable
        , 'proxy'            as cost_basis
        , 'MHSDS'            as cost_source
        , activity_unit
        , total_cost
        , total_activity
    from {{ ref('int_cost_index_mhsds_activity_monthly') }}
)

, csds as (
    select
        sk_patient_id
        , activity_month
        , 'Community'                as service_grouping
        , 'Community Health Contact' as service
        , true                       as is_patient_attributable
        , 'proxy'                    as cost_basis
        , 'CSDS'                     as cost_source
        , 'contact'                  as activity_unit
        , total_cost
        , total_activity
    from {{ ref('int_cost_index_csds_activity_monthly') }}
)

, combined as (
    select * from slam
    union all
    select * from rx
    union all
    select * from mhsds
    union all
    select * from csds
)

select
    sk_patient_id
    , activity_month
    , service_grouping
    , service
    , is_patient_attributable
    , cost_basis
    , cost_source
    , activity_unit
    , sum(total_cost)       as total_cost
    , sum(total_activity)   as total_activity
from combined
group by
    sk_patient_id
    , activity_month
    , service_grouping
    , service
    , is_patient_attributable
    , cost_basis
    , cost_source
    , activity_unit
