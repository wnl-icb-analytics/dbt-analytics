{{
    config(
        materialized='view',
        tags=['fa_checks'])
}}
--This is a high level check to see if the activity submitted is in line with the plan for the financial year and month.
--SOURCE STAGING.SLAM.STG_LSACM
with latestmonth as (
select provider_code, fy_month_submitted as financial_month
FROM {{ ref('int_fa_lsacm_monthly_sub_ytd_latest') }}
--FROM MODELLING.CONTRACTING.INT_FA_LSACM_MONTHLY_SUB_YTD_LATEST
)
--add in variance calculation for activity and cost.
,variance_calc as (
SELECT
       fa.provider_shorthand,
        fa.meta_file_id,
        l.financial_month as Latest_month,
        fa.comm_group,
        SUM(fa.activity_plan) AS activity_plan,
        SUM(fa.activity_actual) AS activity_actual,
        --variance activity
        SUM(fa.activity_actual)-SUM(fa.activity_plan) as diff_activity,
        (SUM(fa.activity_actual)-SUM(fa.activity_plan))/NULLIF(SUM(fa.activity_plan), 0)* 100 AS variance_activity,
        SUM(fa.price_plan) AS cost_plan,
        SUM(fa.price_actual) AS cost_actual,
         --variance cost
        SUM(fa.price_actual)-SUM(fa.price_plan) as diff_cost,
        (SUM(fa.price_actual)-SUM(fa.price_plan))/NULLIF(SUM(fa.price_plan), 0)* 100 AS variance_cost
        FROM {{ ref('int_fa_lsacm_activity_plan_ytd') }} fa
        --FROM MODELLING.CONTRACTING.INT_FA_LSACM_ACTIVITY_PLAN_YTD fa
        LEFT JOIN latestmonth l on fa.provider_code = l.provider_code
        GROUP BY ALL
)
SELECT
    *,
    -- Activity variance flag
    CASE
    WHEN ABS(variance_activity) < 10 THEN '<10%'
    WHEN ABS(variance_activity) < 20 THEN '10-20%'
    ELSE '>=20%'
    END AS variance_activity_threshold,
    -- Activity performance flag
    CASE
        WHEN variance_activity < 0 THEN 'Under Performing'
        WHEN variance_activity > 0 THEN 'Over Performing'
        WHEN variance_activity = 0 THEN 'On Plan'
    END AS variance_activity_flag,
    -- Cost variance flag
    CASE
    WHEN ABS(variance_cost) < 10 THEN '<10%'
    WHEN ABS(variance_cost) < 20 THEN '10-20%'
    ELSE '>=20%'
    END AS variance_cost_threshold,
  -- Cost performance flag
    CASE
        WHEN variance_cost < 0 THEN 'Underspend'
        WHEN variance_cost > 0 THEN 'Overspend'
        WHEN variance_cost = 0 THEN 'On Plan'
    END AS variance_cost_flag
FROM variance_calc