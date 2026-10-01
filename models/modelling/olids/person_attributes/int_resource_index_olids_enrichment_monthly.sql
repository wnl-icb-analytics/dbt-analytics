/*
Monthly OLIDS activity enrichment for the NCL resource-index deep-dive.

Bridge: dim_person_pseudo maps OLIDS person_id to sk_patient_id. Multiple
person_ids can share one sk_patient_id, so measures are aggregated to sk grain
before they join reporting facts.
*/

{{ config(materialized = 'table') }}

with bridge as (
    select person_id, sk_patient_id
    from {{ ref('dim_person_pseudo') }}
),

gp_appts as (
    select
        b.sk_patient_id,
        date_trunc('month', a.start_date)::date as activity_month,
        sum(a.appointment_cost_gbp_nominal)   as gp_appointment_cost,
        count(*)                              as gp_appointments
    from {{ ref('int_appointment_gp_clinical') }} as a
    inner join bridge as b
        on a.person_id = b.person_id
    where a.is_attended
      and a.start_date is not null
    group by 1, 2
),

olids_rx as (
    select
        b.sk_patient_id,
        r.activity_month,
        sum(r.prescribing_cost_modelled) as olids_prescribing_cost_modelled,
        sum(r.prescription_orders)       as olids_prescription_orders
    from {{ ref('int_cost_index_olids_prescribing_monthly') }} as r
    inner join bridge as b
        on r.person_id = b.person_id
    group by 1, 2
),

months as (
    select sk_patient_id, activity_month from gp_appts
    union
    select sk_patient_id, activity_month from olids_rx
)

select
    m.sk_patient_id,
    m.activity_month,
    coalesce(gp_appts.gp_appointment_cost, 0)             as gp_appointment_cost,
    coalesce(gp_appts.gp_appointments, 0)                 as gp_appointments,
    coalesce(olids_rx.olids_prescribing_cost_modelled, 0) as olids_prescribing_cost_modelled,
    coalesce(olids_rx.olids_prescription_orders, 0)       as olids_prescription_orders
from months as m
left join gp_appts
    on m.sk_patient_id = gp_appts.sk_patient_id
    and m.activity_month = gp_appts.activity_month
left join olids_rx
    on m.sk_patient_id = olids_rx.sk_patient_id
    and m.activity_month = olids_rx.activity_month
