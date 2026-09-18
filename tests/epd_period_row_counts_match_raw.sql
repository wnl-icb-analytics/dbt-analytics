-- stg_epd_pc_meds replaces complete processing periods incrementally. The EPD
-- source carries no unique row identifier and the staging model preserves
-- every row in the selected delivery version. Its grain cannot be tested with
-- a key. Each period must hold exactly the rows in its raw delivery: a period
-- the incremental replacement duplicated, dropped or left stale fails here.
--
-- Returns one row per processing period whose row count differs from raw.

with v2_periods as (
    select processed_period, count(*) as row_count
    from {{ ref('raw_epd_pc_medsv2') }}
    group by processed_period
), raw_periods as (
    select processed_period, count(*) as row_count
    from {{ ref('raw_epd_pc_medsv1') }} as v1
    where not exists (
        select 1 from v2_periods
        where equal_null(v2_periods.processed_period, v1.processed_period)
    )
    group by processed_period
    union all
    select processed_period, row_count from v2_periods
),

staged_periods as (
    select processed_period, count(*) as row_count
    from {{ ref('stg_epd_pc_meds') }}
    group by processed_period
)

select
    coalesce(raw_periods.processed_period, staged_periods.processed_period) as processed_period,
    raw_periods.row_count as raw_row_count,
    staged_periods.row_count as staged_row_count
from raw_periods
full outer join staged_periods
    on equal_null(raw_periods.processed_period, staged_periods.processed_period)
where raw_periods.row_count is distinct from staged_periods.row_count
