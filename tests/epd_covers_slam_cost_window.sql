-- Warns when EPD prescribing does not cover the SLAM rolling cost window in
-- fct_person_cost_index_monthly. The union clips prescribing to the SLAM window,
-- so a lagging EPD feed silently thins the cost base (as of 2026-07 the feed
-- is ~12 months behind and contributes ~1 month of the window); when it
-- catches up, months of prescribing spend switch on with no code change and
-- move every resource index. This test makes that state visible.
--
-- Returns one row per window month with SLAM cost but no EPD cost.
{{ config(severity = 'warn') }}

with months as (
    select activity_month,
           max(iff(cost_source = 'SLAM', 1, 0)) as has_slam,
           max(iff(cost_source = 'EPD', 1, 0))  as has_epd
    from {{ ref('fct_person_cost_index_monthly') }}
    group by activity_month
)

select activity_month as month_missing_prescribing
from months
where has_slam = 1
  and has_epd = 0
