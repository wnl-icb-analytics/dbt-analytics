{{
    config(
        materialized='table',
        cluster_by=['month_end_date']
    )
}}

-- LTC LCS: monthly population counts by overall risk group, for dashboard trend
-- charts. One row per month_end_date x overall_risk_group.

select
    month_end_date,
    month_year_label,
    financial_year,
    financial_quarter,
    overall_risk_group,
    count(distinct person_id) as person_count
from {{ ref('int_ltc_lcs_rs_monthly_position') }}
group by
    month_end_date,
    month_year_label,
    financial_year,
    financial_quarter,
    overall_risk_group
