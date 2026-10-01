{{
    config(
        materialized='table',
        cluster_by=['month_end_date']
    )
}}

-- LTC LCS: monthly case-finding population counts per indicator, grouped by
-- condition, for dashboard trend charts. One row per month_end_date x condition
-- x indicator. A person in multiple indicators is counted once per indicator.

select
    month_end_date,
    month_year_label,
    financial_year,
    financial_quarter,
    condition,
    indicator_code,
    count(distinct person_id) as person_count
from {{ ref('int_ltc_lcs_cf_monthly_membership') }}
group by
    month_end_date,
    month_year_label,
    financial_year,
    financial_quarter,
    condition,
    indicator_code
