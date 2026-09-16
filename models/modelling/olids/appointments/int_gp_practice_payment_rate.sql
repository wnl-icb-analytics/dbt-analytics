{{
    config(
        materialized='table',
        tags=['intermediate', 'appointment', 'gp']
    )
}}

/*
NHS Payments to General Practice spread over attended clinical appointments.

Grain: one row per practice per fiscal year with appointments in
int_appointment_gp_clinical.

payment_per_attended_appointment_gbp is the practice's total NHS payments for
the year divided by its attended clinical appointments that year. It is the
budget-based alternative to the PSSRU per-minute unit costs on
int_appointment_gp_clinical, which stay as they are. Every pound the practice
received, including premises, QOF, enhanced services and dispensing, lands on
an attended appointment, so the rate answers "what did the NHS pay this
practice per appointment delivered", not "what does an appointment of this
type cost". The two are not interchangeable; use this one when comparing with
a partner whose GP cost is a budget spread over activity.

NHS Payments publishes after the year ends. Fiscal years after a practice's
latest published year carry that year's rate forward, uplifted by the GDP
deflator from uk_cost_indices, and are flagged is_carried_forward. Years before
a practice's first published year, and practices with no published payments,
have no rate.
*/

with attended as (
    select
        publisher_organisation_code as practice_code,
        fiscal_year_start,
        count_if(is_attended) as attended_appointments
    from {{ ref('int_appointment_gp_clinical') }}
    where publisher_organisation_code is not null
      and fiscal_year_start is not null
    group by 1, 2
),

payments as (
    select
        practice_code,
        year(financial_year_start) as fiscal_year_start,
        total_nhs_payments
    from {{ ref('stg_nhs_payments_gp_practice_payments') }}
),

-- Each practice's latest published year, with that year's attended count,
-- is the basis for carrying the rate forward.
practice_latest as (
    select
        p.practice_code,
        p.fiscal_year_start,
        p.total_nhs_payments,
        a.attended_appointments
    from payments as p
    left join attended as a
        on a.practice_code = p.practice_code
       and a.fiscal_year_start = p.fiscal_year_start
    qualify row_number() over (
        partition by p.practice_code
        order by p.fiscal_year_start desc
    ) = 1
),

deflator as (
    select fiscal_year_start, gdp_deflator
    from {{ ref('uk_cost_indices') }}
)

select
    a.practice_code,
    a.fiscal_year_start,
    a.attended_appointments,
    coalesce(
        p.fiscal_year_start is null and pl.fiscal_year_start < a.fiscal_year_start,
        false
    ) as is_carried_forward,
    case
        when p.fiscal_year_start is not null then p.fiscal_year_start
        when pl.fiscal_year_start < a.fiscal_year_start then pl.fiscal_year_start
    end as payments_fiscal_year_start,
    case
        when p.fiscal_year_start is not null then p.total_nhs_payments
        when pl.fiscal_year_start < a.fiscal_year_start
            then pl.total_nhs_payments * d.gdp_deflator / dl.gdp_deflator
    end as total_nhs_payments_gbp,
    case
        when p.fiscal_year_start is not null
            then p.total_nhs_payments / nullif(a.attended_appointments, 0)
        when pl.fiscal_year_start < a.fiscal_year_start
            then pl.total_nhs_payments / nullif(pl.attended_appointments, 0)
                 * d.gdp_deflator / dl.gdp_deflator
    end as payment_per_attended_appointment_gbp
from attended as a
left join payments as p
    on p.practice_code = a.practice_code
   and p.fiscal_year_start = a.fiscal_year_start
left join practice_latest as pl
    on pl.practice_code = a.practice_code
left join deflator as d
    on d.fiscal_year_start = a.fiscal_year_start
left join deflator as dl
    on dl.fiscal_year_start = pl.fiscal_year_start
