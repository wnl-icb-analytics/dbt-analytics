-- stg_epd_pc_meds replaces complete processing periods incrementally. The EPD
-- source carries no unique row identifier and the staging model preserves
-- every source row, so its grain cannot be tested with a key. Instead, every
-- processing period must hold exactly the rows the raw feed holds: a period
-- the incremental replacement duplicated, dropped or left stale fails here.
--
-- Returns one row per processing period whose row count differs from raw.

with raw_periods as (
    select processed_period, count(*) as row_count
    from {{ ref('raw_epd_pc_medsv1') }}
    group by processed_period
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
