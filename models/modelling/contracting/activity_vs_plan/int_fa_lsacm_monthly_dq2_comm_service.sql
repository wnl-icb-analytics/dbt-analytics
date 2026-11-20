{{
    config(
        materialized='view',
        tags=['fa_checks'])
}}
--list activity vs plan by commissioner service category code, service code and spec_comm_flag for the latest LSACM monthly submission used for DQ check 2.
with latestmonth as (
select provider_code, fy_month_submitted as financial_month
FROM {{ ref('int_fa_lsacm_monthly_sub_ytd_latest') }}
--FROM MODELLING.CONTRACTING.INT_FA_LSACM_MONTHLY_SUB_YTD_LATEST
)

SELECT
   fa.provider_shorthand,
    fa.meta_file_id,
    l.financial_month as Latest_month,
    fa.comm_group,
    fa.spec_comm_flag,
      -- DQ issue
    CASE 
        WHEN service_code_dq = 'Valid' AND cscc_funding_status <> spec_comm_flag THEN 'CSCC Allocation'
        WHEN cscc_funding_status = spec_comm_flag AND cscc_dq <> 'Valid' THEN 'CSCC Code Invalid'
        WHEN cscc_funding_status <> spec_comm_flag AND service_code_dq in ('Missing Code','Invalid Code') THEN 'Service Code Missing/Invalid' 
        WHEN cscc_funding_status = spec_comm_flag AND service_code_dq = 'Code fix required' THEN 'Service Code Incorrect Case'
    ELSE 'No issues' END AS dq_issue,
    --commissioned_service_category
    CONCAT(fa.commissioned_service_category_code, ' - ', fa.cscc_name) as acm_commissioned_service,
    fa.cscc_funding_status as acm_funding_status,
    fa.cscc_dq as cscc_code_dq,
    --service_code
    fa.service_code_funding_status,
    CONCAT(fa.icb_delegation_26_27, ' - ', fa.service_code_funding_status) as service_delegation2627,
    CONCAT(fa.service_code, ' - ', COALESCE(fa.service_code_desc, 'NULL')) as acm_service_code,
    fa.service_code_dq,
    SUM(fa.activity_actual) AS activity_actual,
    SUM(fa.activity_plan) AS activity_plan,
     --variance activity
    SUM(fa.activity_actual)-SUM(fa.activity_plan) as diff_activity,
    (SUM(fa.activity_actual)-SUM(fa.activity_plan))/NULLIF(SUM(fa.activity_plan), 0)* 100 AS variance_activity,
    SUM(fa.price_actual) AS price_actual,
    SUM(fa.price_plan) AS price_plan,
     --variance cost
    SUM(fa.price_actual)-SUM(fa.price_plan) as diff_cost,
    (SUM(fa.price_actual)-SUM(fa.price_plan))/NULLIF(SUM(fa.price_plan), 0)* 100 AS variance_cost
FROM {{ ref('int_fa_lsacm_activity_plan_ytd') }} fa
--FROM DEV__MODELLING.CONTRACTING.INT_FA_LSACM_activity_plan_YTD fa
LEFT JOIN latestmonth l on fa.provider_code = l.provider_code
group by all
order by provider_shorthand