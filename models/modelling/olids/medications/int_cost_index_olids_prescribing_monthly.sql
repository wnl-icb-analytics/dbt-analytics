/*
OLIDS prescribing cost per person per month (NCL only - OLIDS is EMIS NCL).

Uses MEDICATION_ORDER.estimated_cost - EMIS's per-order cost estimate.
Checked May 2025 - Apr 2026: 100% populated, 95.7% positive. Window total
GBP 239m vs GBP 238m when priced at EPD mean actual cost per item by BNF
code (0.4% apart). Basis is an estimate, not reimbursed spend.

Effective date: ~410k orders carry a corrupt future clinical_effective_date
(e.g. year 2621) that would bin into far-future months and drop out of every
window. Fall back to date_recorded (the system record date) when the clinical
date is null or in the future, recovering ~GBP 1.6m of in-window spend.

orders_costed counts orders with a positive estimate.
*/

{{ config(materialized = 'table') }}

with orders as (
    select
        o.person_id,
        case
            when o.clinical_effective_date is null
                 or o.clinical_effective_date > current_date()
                then o.date_recorded
            else o.clinical_effective_date
        end                            as effective_date,
        o.estimated_cost
    from {{ ref('stg_olids_medication_order') }} as o
    where o.person_id is not null
)

select
    person_id,
    date_trunc('month', effective_date)::date as activity_month,
    count(*)                                   as prescription_orders,
    count_if(estimated_cost > 0)              as orders_costed,
    round(sum(coalesce(estimated_cost, 0)), 2) as prescribing_cost_modelled
from orders
where effective_date is not null
  and effective_date <= current_date()
group by 1, 2
