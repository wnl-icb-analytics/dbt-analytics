{{
    config(
        materialized='table',
        tags=['intermediate', 'appointment', 'gp']
    )
}}

/*
NHS Payments to General Practice per practice and fiscal year, and the rate
per attended clinical appointment it implies.

Grain: one row per WNL practice (int_organisation_borough_mapping, sub-ICBs
93C and W2U3Z) per fiscal year in scope, plus any other practice with
appointments in int_appointment_gp_clinical in those years. Scope is the
fiscal years overlapping the latest 60 months of appointments, the same
rolling window as fct_gp_appointment_costs; OLIDS keeps little history
before that, so earlier years cannot be priced against a full year's money.

payment_per_attended_appointment_gbp is total_nhs_payments_gbp divided by
the practice's attended clinical appointments in the same year, with the
current partial year annualised by days elapsed. It is the budget-based
alternative to the PSSRU per-minute costs on int_appointment_gp_clinical,
which stay as they are: everything the practice was paid (global sum, QOF,
enhanced services, premises; prescribing costs are outside the file and
dispensing fees are negligible in London) lands on attended appointments,
so it answers "what did the NHS pay per appointment
delivered", not "what does this type of appointment cost". Use it where GP
cost must be a budget spread over activity, for example beside a partner
whose GP cost is built that way. NWL practices have payments but no OLIDS
appointments here, so their rate is null and a consumer divides the
payments by its own activity.

payments_basis says where the money comes from:
  * published        the practice's NHS Payments row for that year. A row is
                     usable when the appointment feed covers the year
                     through 31 March and it pays at least £20 per
                     registered patient; below that it is a partial or
                     misattributed record, not income, and is ignored.
  * carried_forward  the practice's latest usable earlier year, uplifted by
                     the GDP deflator from uk_cost_indices, provided it is
                     one of the two most recent published years. NHS
                     Payments publishes about a year after the year ends;
                     a code absent for longer has closed or merged and its
                     old money must not be carried on.
  * imputed          no usable year at or before this one, as for new or
                     merged codes: the sub-ICB's £ per registered patient
                     for the latest published year, uplifted, times the
                     practice's registered list from
                     practice_weighted_population.

NHS Payments stopped attributing PCN categories (workforce, enhanced access,
IIF) to practices from 2023/24, so from then the basis leaves out most ARRS
funding. attended_per_registered_patient is exposed because a practice with
few recorded appointments per patient reads high.
*/

with feed as (
    select
        max(start_date)::date as last_appointment_date,
        year(dateadd(month, -59, max(start_date)))
            - iff(month(dateadd(month, -59, max(start_date))) < 4, 1, 0)
            as first_fiscal_year_start,
        year(max(start_date)) - iff(month(max(start_date)) < 4, 1, 0)
            as last_fiscal_year_start
    from {{ ref('int_appointment_gp_clinical') }}
),

fiscal_years as (
    select f.first_fiscal_year_start + g.n as fiscal_year_start
    from feed as f
    cross join (
        select row_number() over (order by seq4()) - 1 as n
        from table(generator(rowcount => 12))
    ) as g
    where f.first_fiscal_year_start + g.n <= f.last_fiscal_year_start
),

wnl_practices as (
    select practice_code, sub_icb_code
    from {{ ref('int_organisation_borough_mapping') }}
    where sub_icb_code in ('93C', 'W2U3Z')
),

-- Attended appointments per practice-year; the year in progress is
-- annualised so its rate is not inflated by the months still to come.
attended as (
    select
        a.publisher_organisation_code as practice_code,
        a.fiscal_year_start,
        count_if(a.is_attended) as attended_appointments,
        count_if(a.is_attended) * 365.0 / least(
            365,
            datediff(day, date_from_parts(a.fiscal_year_start, 4, 1), f.last_appointment_date) + 1
        ) as attended_appointments_annualised
    from {{ ref('int_appointment_gp_clinical') }} as a
    cross join feed as f
    where a.publisher_organisation_code is not null
      and a.fiscal_year_start between f.first_fiscal_year_start and f.last_fiscal_year_start
    group by 1, 2, f.last_appointment_date
),

practice_years as (
    select p.practice_code, p.sub_icb_code, y.fiscal_year_start
    from wnl_practices as p
    cross join fiscal_years as y
    union
    select a.practice_code, w.sub_icb_code, a.fiscal_year_start
    from attended as a
    left join wnl_practices as w
        on w.practice_code = a.practice_code
),

-- Usable published years only (see header).
payments as (
    select
        p.practice_code,
        p.commissioner_code as sub_icb_code,
        year(p.financial_year_start) as fiscal_year_start,
        p.total_nhs_payments,
        p.registered_patients
    from {{ ref('stg_nhs_payments_gp_practice_payments') }} as p
    cross join feed as f
    where dateadd(year, 1, p.financial_year_start) - 1 <= f.last_appointment_date
      and p.total_nhs_payments / p.registered_patients >= 20
),

-- Sub-ICB £ per registered patient by published year, for imputation.
sub_icb_rate as (
    select
        sub_icb_code,
        fiscal_year_start,
        sum(total_nhs_payments) / sum(registered_patients) as payments_per_registered_patient
    from payments
    group by 1, 2
),

-- Latest usable earlier year per practice-year, for carrying forward, and
-- only from the two most recent published years so closed codes lapse.
carried as (
    select
        py.practice_code,
        py.fiscal_year_start,
        pl.fiscal_year_start as source_fiscal_year_start,
        pl.total_nhs_payments,
        pl.registered_patients
    from practice_years as py
    join payments as pl
        on pl.practice_code = py.practice_code
       and pl.fiscal_year_start < py.fiscal_year_start
       and pl.fiscal_year_start >= (select max(fiscal_year_start) - 1 from payments)
    qualify row_number() over (
        partition by py.practice_code, py.fiscal_year_start
        order by pl.fiscal_year_start desc
    ) = 1
),

-- Latest published sub-ICB rate at or before each year, for imputation.
imputed_rate as (
    select
        py.practice_code,
        py.fiscal_year_start,
        s.fiscal_year_start as source_fiscal_year_start,
        s.payments_per_registered_patient
    from practice_years as py
    join sub_icb_rate as s
        on s.sub_icb_code = py.sub_icb_code
       and s.fiscal_year_start <= py.fiscal_year_start
    qualify row_number() over (
        partition by py.practice_code, py.fiscal_year_start
        order by s.fiscal_year_start desc
    ) = 1
),

-- Registered list for imputation: the base year at or before the fiscal
-- year, else the next one, and never more than a year old, so a closed
-- code is not imputed from a list it no longer has.
imputed_list as (
    select
        py.practice_code,
        py.fiscal_year_start,
        w.registered_patients
    from practice_years as py
    join {{ ref('practice_weighted_population') }} as w
        on w.practice_code = py.practice_code
       and year(w.financial_year_start) >= py.fiscal_year_start - 1
    qualify row_number() over (
        partition by py.practice_code, py.fiscal_year_start
        order by iff(year(w.financial_year_start) <= py.fiscal_year_start, 0, 1),
                 abs(year(w.financial_year_start) - py.fiscal_year_start)
    ) = 1
),

deflator as (
    select fiscal_year_start, gdp_deflator
    from {{ ref('uk_cost_indices') }}
),

priced as (
    select
        py.practice_code,
        py.fiscal_year_start,
        py.sub_icb_code,
        a.attended_appointments,
        a.attended_appointments_annualised,
        case
            when p.practice_code is not null then 'published'
            when c.practice_code is not null then 'carried_forward'
            when ir.practice_code is not null and il.registered_patients is not null then 'imputed'
        end as payments_basis,
        case
            when p.practice_code is not null then p.fiscal_year_start
            when c.practice_code is not null then c.source_fiscal_year_start
            when ir.practice_code is not null and il.registered_patients is not null then ir.source_fiscal_year_start
        end as payments_fiscal_year_start,
        case
            when p.practice_code is not null then p.total_nhs_payments
            when c.practice_code is not null then c.total_nhs_payments * d.gdp_deflator / dc.gdp_deflator
            when ir.practice_code is not null and il.registered_patients is not null
                then ir.payments_per_registered_patient * d.gdp_deflator / di.gdp_deflator * il.registered_patients
        end as total_nhs_payments_gbp,
        case
            when p.practice_code is not null then p.registered_patients
            when c.practice_code is not null then c.registered_patients
            else il.registered_patients
        end as registered_patients
    from practice_years as py
    left join attended as a
        on a.practice_code = py.practice_code
       and a.fiscal_year_start = py.fiscal_year_start
    left join payments as p
        on p.practice_code = py.practice_code
       and p.fiscal_year_start = py.fiscal_year_start
    left join carried as c
        on c.practice_code = py.practice_code
       and c.fiscal_year_start = py.fiscal_year_start
    left join imputed_rate as ir
        on ir.practice_code = py.practice_code
       and ir.fiscal_year_start = py.fiscal_year_start
    left join imputed_list as il
        on il.practice_code = py.practice_code
       and il.fiscal_year_start = py.fiscal_year_start
    left join deflator as d
        on d.fiscal_year_start = py.fiscal_year_start
    left join deflator as dc
        on dc.fiscal_year_start = c.source_fiscal_year_start
    left join deflator as di
        on di.fiscal_year_start = ir.source_fiscal_year_start
)

select
    practice_code,
    fiscal_year_start,
    sub_icb_code,
    payments_basis,
    payments_fiscal_year_start,
    total_nhs_payments_gbp,
    registered_patients,
    attended_appointments,
    attended_appointments / nullif(registered_patients, 0) as attended_per_registered_patient,
    total_nhs_payments_gbp / nullif(attended_appointments_annualised, 0)
        as payment_per_attended_appointment_gbp
from priced
