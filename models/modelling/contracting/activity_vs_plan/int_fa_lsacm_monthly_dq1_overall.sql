{{
    config(
        materialized='view',
        tags=['fa_checks'])
}}
--First check of activity versus plan by commissioner code for WNL only. This is a high level check to see if the activity submitted is in line with the plan for the financial year and month.
--SOURCE STAGING.SLAM.STG_LSACM
with latestmonth as (
select provider_code, max(fy_month_submitted) as financial_month
FROM {{ ref('int_fa_lsacm_monthly_sub_ytd_latest') }}
--FROM MODELLING.CONTRACTING.INT_FA_LSACM_MONTHLY_SUB_YTD_LATEST
group by provider_code
)

SELECT
        fa.provider_shorthand,
        fa.meta_file_id,
        l.financial_month as Latest_month,
        fa.commissioner_code,
        fa.commissioner_name,
        SUM(fa.activity_actual) AS activity_actual,
        SUM(fa.activity_plan) AS activity_plan,
        SUM(fa.price_actual) AS price_actual,
        SUM(fa.price_plan) AS price_plan
        FROM {{ ref('int_fa_lsacm_activity_plan_ytd') }} fa
        --FROM MODELLING.CONTRACTING.INT_FA_LSACM_ACTIVITY_PLAN_YTD fa
        LEFT JOIN latestmonth l on fa.provider_code = l.provider_code
        GROUP BY ALL
        ORDER BY fa.provider_shorthand, fa.commissioner_code