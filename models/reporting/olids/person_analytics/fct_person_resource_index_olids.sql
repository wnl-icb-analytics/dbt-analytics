-- NCL deep-dive: fct_person_resource_index (registered sub-ICB 93C) enriched
-- with OLIDS primary-care context - Cambridge comorbidity score, LTC register
-- count, frailty, GP appointment cost and the EPD prescribing breakout.
-- One row per patient, same grain as the base fact.
--
-- Cost columns: actual_cost_12m is SLAM acute only (EPD is held out of the
-- resource index while stale). Whole-person cost adds the two primary-care
-- streams the deep-dive can price:
--   total_cost_whole_person_12m = actual (SLAM) + OLIDS prescribing estimate
--                                 + GP appointment cost (PSSRU).
--   total_cost_incl_gp_appointments_12m = actual + GP appointments only.
{{ config(materialized = 'table') }}

with bounds as (
    select
        max(activity_month)                      as window_end_month,
        dateadd(month, -11, max(activity_month)) as window_start_month
    from {{ ref('int_cost_index_slam_activity_monthly') }}
),

activity_12m as (
    select
        e.sk_patient_id,
        sum(e.gp_appointment_cost)             as gp_appointment_cost_12m,
        sum(e.gp_appointments)                 as gp_appointments_12m,
        sum(e.olids_prescribing_cost_modelled) as olids_prescribing_cost_12m_modelled,
        sum(e.olids_prescription_orders)       as olids_prescription_orders_12m
    from {{ ref('int_resource_index_olids_enrichment_monthly') }} as e
    cross join bounds as b
    where e.activity_month between b.window_start_month and b.window_end_month
    group by 1
)

select
    f.*,
    p.sk_patient_id is not null                   as has_olids_record,
    p.cambridge_comorbidity_score,
    coalesce(p.ltc_count, 0)                      as ltc_count,
    coalesce(p.qof_ltc_count, 0)                  as qof_ltc_count,
    p.frailty_severity,
    coalesce(a.gp_appointment_cost_12m, 0)        as gp_appointment_cost_12m,
    coalesce(a.gp_appointments_12m, 0)            as gp_appointments_12m,
    coalesce(a.olids_prescribing_cost_12m_modelled, 0) as olids_prescribing_cost_12m_modelled,
    coalesce(a.olids_prescription_orders_12m, 0)  as olids_prescription_orders_12m,
    f.actual_cost_12m
        + coalesce(a.gp_appointment_cost_12m, 0)  as total_cost_incl_gp_appointments_12m,
    f.actual_cost_12m
        + coalesce(a.olids_prescribing_cost_12m_modelled, 0)
        + coalesce(a.gp_appointment_cost_12m, 0)  as total_cost_whole_person_12m
from {{ ref('fct_person_resource_index') }} as f
left join {{ ref('int_resource_index_olids_profile') }} as p
    on f.sk_patient_id = p.sk_patient_id
left join activity_12m as a
    on f.sk_patient_id = a.sk_patient_id
where f.registered_sub_icb_code = '93C'
