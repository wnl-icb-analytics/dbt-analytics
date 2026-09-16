-- Payment-basis appointment cost must add back to the published NHS
-- payments for every practice year the fact covers in full. Carried-forward
-- years are excluded (their total is an uplift, not a published figure), as
-- are years that start before the fact's rolling window. Tolerance 0.5%
-- covers per-row rounding.
with window_start as (
    select min(report_month) as first_month
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
cross join window_start as w
where not r.is_carried_forward
  and r.payment_per_attended_appointment_gbp is not null
  and date_from_parts(f.fiscal_year_start, 4, 1) >= w.first_month
  and abs(f.total_payment_cost_gbp - r.total_nhs_payments_gbp)
      > 0.005 * r.total_nhs_payments_gbp
