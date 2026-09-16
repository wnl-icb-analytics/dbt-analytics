{{
    config(
        materialized='table',
        tags=['fact', 'gp_costs', 'kpi']
    )
}}

/*
GP appointment cost on the NHS Payments basis.

Grain: one row per practice per month per practitioner_role_group and
sds_role_group, the same grain and rolling 60-month scope as
fct_gp_appointment_costs, so the PSSRU and payments costings can be read
side by side.

Cost is attended appointments x the practice's
payment_per_attended_appointment_gbp for the fiscal year, from
int_gp_practice_payment_rate. DNAs carry no cost. total_payment_cost_gbp is
null where the practice has no NHS Payments record. is_carried_forward marks
months priced on an uplifted earlier year because NHS Payments has not
published the fiscal year yet.
*/

select
    a.publisher_organisation_code as practice_code,
    date_trunc('month', a.start_date) as report_month,
    a.practitioner_role_group,
    a.sds_role_group,

    -- Functionally determined by report_month.
    max(a.fiscal_year_start) as fiscal_year_start,

    -- One rate per practice and fiscal year, so any_value is safe.
    any_value(r.is_carried_forward) as is_carried_forward,
    any_value(r.payment_per_attended_appointment_gbp) as payment_per_attended_appointment_gbp,

    count(*) as appointment_count,
    count_if(a.is_attended) as attended_count,
    round(
        count_if(a.is_attended) * any_value(r.payment_per_attended_appointment_gbp),
        2
    ) as total_payment_cost_gbp

from {{ ref('int_appointment_gp_clinical_recent') }} as a
left join {{ ref('int_gp_practice_payment_rate') }} as r
    on r.practice_code = a.publisher_organisation_code
   and r.fiscal_year_start = a.fiscal_year_start
group by
    a.publisher_organisation_code,
    date_trunc('month', a.start_date),
    a.practitioner_role_group,
    a.sds_role_group
