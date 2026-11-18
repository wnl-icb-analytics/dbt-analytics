-- Asserts the qadmissions_lab_thresholds seed contains every row that
-- downstream models pivot on. If a required row is missing, the MAX(CASE ...)
-- pivot silently returns NULL, the value > / < threshold comparisons become
-- NULL, and the outer COALESCE(..., FALSE) flips the feature flag to FALSE
-- for every person. The covered pivots are:
--   qadmissions_input_features.lab_thresholds: (haemoglobin, low), (platelets, high)
--   int_lft_latest.thresholds:               (alt, high), (ggt, high), (bilirubin, high)
--
-- The test fails when the SELECT returns rows. It returns one row per
-- required (measurement, direction) combination that is absent from the seed.

with required(measurement, direction) as (
    select 'haemoglobin', 'low'  union all
    select 'platelets',   'high' union all
    select 'alt',         'high' union all
    select 'ggt',         'high' union all
    select 'bilirubin',   'high'
),
present as (
    select distinct measurement, direction
    from {{ ref('qadmissions_lab_thresholds') }}
    where threshold is not null
)
select r.measurement, r.direction
from required r
left join present p
  on p.measurement = r.measurement
 and p.direction   = r.direction
where p.measurement is null
