-- Indirect standardisation self-calibration: sum(expected) must equal
-- sum(actual) across the whole exposure population, because the age-sex
-- curve is built from the same cost and exposure it is applied to. Drift
-- means the curve and the actual have diverged in window or exclusions
-- (e.g. one holds out stale EPD and the other does not).
--
-- Returns one row when the totals differ beyond float rounding.
select actual_total, expected_total
from (
    select
        sum(actual_cost_12m)   as actual_total,
        sum(expected_cost_12m) as expected_total
    from {{ ref('fct_person_resource_index') }}
)
where div0(abs(actual_total - expected_total), actual_total) > 1e-6
