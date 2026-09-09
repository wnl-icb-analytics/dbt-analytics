-- UKHFD GP practice allocation metrics. One row per practice, workbook title,
-- metric and allocation year. Metric vocabulary varies by allocations round.
--
-- Source: UKHFD.Comm_Allocations.fact_Weighted_Regs_By_GP_Practice
--   source('ukhfd_comm_allocations', 'weighted_regs_by_gp_practice')
--   -> raw_ukhfd_weighted_regs_by_gp_practice
select
    gp_practice_code as practice_code,
    title,
    metric_name,
    metric_value::float as metric_value,
    effective_snapshot_date as financial_year_start,
    data_source_file_for_this_snapshot_version as source_file_version
from {{ ref('raw_ukhfd_weighted_regs_by_gp_practice') }}
where gp_practice_code is not null
  and metric_value is not null
