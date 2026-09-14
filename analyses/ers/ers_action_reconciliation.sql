-- Counts at month and action-code grain; no identifiers are returned.
with expected as (
    select date_trunc('month', action_dt_tm) as action_month,
        action_cd::varchar as action_code, count(*) as row_count
    from {{ ref('stg_ers_ubrn_action') }}
    group by action_month, action_code
), actual as (
    select date_trunc('month', action_at) as action_month,
        action_code, count(*) as row_count
    from {{ ref('fct_ers_referral_action') }}
    group by action_month, action_code
)
select count(*) as month_code_groups,
    count_if(e.row_count is distinct from a.row_count) as differing_groups,
    sum(coalesce(e.row_count, 0)) as source_rows,
    sum(coalesce(a.row_count, 0)) as fact_rows
from expected e
full outer join actual a
    on equal_null(e.action_month, a.action_month) and equal_null(e.action_code, a.action_code)
