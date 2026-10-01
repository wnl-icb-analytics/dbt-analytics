-- Asserts every annual anchor date configured in
-- fct_person_segment_annual_history is still present in
-- fct_person_segment_by_month. The parent holds a rolling 60 completed
-- month-ends measured from the build date, so the earliest anchor eventually
-- drops out of that window. When it does, MAX(IFF(end_date = ...)) in the
-- annual model matches nothing and the published status_, segment_number_ and
-- population flag columns for that year read null for every person. The only
-- other signal is a not_null test failing on 100% of rows, which reads as a
-- data incident rather than an anchor that needs retiring or a wider window.
--
-- Keep this list in step with the annual_snapshots list in
-- fct_person_segment_annual_history.sql.
--
-- The test fails when the SELECT returns rows. It returns one row per
-- configured anchor date absent from the monthly history, and no person data.

with anchors (anchor_date) as (
    select '2022-07-31'::date union all
    select '2023-07-31'::date union all
    select '2024-07-31'::date union all
    select '2025-07-31'::date union all
    select '2026-07-31'::date
),

monthly_month_ends as (
    select distinct end_date
    from {{ ref('fct_person_segment_by_month') }}
)

select a.anchor_date as missing_anchor_date
from anchors as a
left join monthly_month_ends as m
    on m.end_date = a.anchor_date
where m.end_date is null
