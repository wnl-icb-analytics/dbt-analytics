-- Payment-basis appointment cost must add back to the published NHS
-- payments for every practice year the fact covers in full. Carried-forward
-- and imputed years are excluded (their total is an estimate, not a
-- published figure), as are years that start before the fact's rolling
-- window or end after its latest month. Tolerance 0.5% covers per-row
-- rounding.
with window_bounds as (
    select
        min(report_month) as first_month,
        max(report_month) as last_month
    from {{ ref('fct_gp_appointment_payment_costs') }}
),

fact as (
    select
        practice_code,
        fiscal_year_start,
        sum(total_payment_cost_gbp) as total_payment_cost_gbp
    from {{ ref('fct_gp_appointment_payment_costs') }}
    group by 1, 2
)

select
    f.practice_code,
    f.fiscal_year_start,
    f.total_payment_cost_gbp,
    r.total_nhs_payments_gbp
from fact as f
join {{ ref('int_gp_practice_payment_rate') }} as r
    on r.practice_code = f.practice_code
   and r.fiscal_year_start = f.fiscal_year_start
cross join window_bounds as w
where r.payments_basis = 'published'
  and r.payment_per_attended_appointment_gbp is not null
  and date_from_parts(f.fiscal_year_start, 4, 1) >= w.first_month
  and dateadd(month, 11, date_from_parts(f.fiscal_year_start, 4, 1)) <= w.last_month
  and abs(f.total_payment_cost_gbp - r.total_nhs_payments_gbp)
      > 0.005 * r.total_nhs_payments_gbp
