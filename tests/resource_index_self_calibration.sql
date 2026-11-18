-- Indirect standardisation self-calibration: sum(expected) must equal
-- sum(actual) across the whole exposure population, because the age-sex
-- curve is built from the same cost and exposure it is applied to. Drift
-- means the curve and the actual have diverged in window or exclusions
-- (e.g. one holds out stale EPD and the other does not).
--
-- Empty, null or zero actual totals cannot establish self-calibration.
-- Returns one row for invalid totals or drift beyond float rounding.
{{ config(severity = 'error') }}

select actual_total, expected_total
from (
    select
        sum(actual_cost_12m)   as actual_total,
        sum(expected_cost_12m) as expected_total
    from {{ ref('fct_person_resource_index') }}
)
where actual_total is null
   or expected_total is null
   or actual_total = 0
   or abs(actual_total - expected_total) / nullif(abs(actual_total), 0) > 1e-6
