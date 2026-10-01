-- UKHFD GP need-index base values. One row per practice, metric and base year.
--
-- Source: UKHFD.Comm_Allocations.fact_Weighted_Regs_By_GP_Practice_Base
--   source('ukhfd_comm_allocations', 'weighted_regs_by_gp_practice_base')
--   -> raw_ukhfd_weighted_regs_by_gp_practice_base
select
    gp_practice_code as practice_code,
    metric_name,
    metric_value::float as metric_value,
    effective_snapshot_date as financial_year_start,
    data_source_file_for_this_snapshot_version as source_file_version
from {{ ref('raw_ukhfd_weighted_regs_by_gp_practice_base') }}
where title = 'gp_need_index_base'
  and gp_practice_code is not null
  and metric_value is not null
