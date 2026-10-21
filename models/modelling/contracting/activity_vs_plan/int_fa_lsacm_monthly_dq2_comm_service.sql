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
    fa.spec_comm_flag,
    --fa.commissioner_code,
    fa.commissioned_service_category_code,
    fa.cscc_funding_status,
    fa.cscc_dq,
    fa.service_code,
    fa.service_code_funding_status,
    fa.service_code_dq,
    CONCAT(fa.commissioned_service_category_code, ' - ', fa.cscc_name) as commissioned_service,
    CONCAT(fa.service_code, ' - ', COALESCE(fa.service_code_desc, 'NULL')) as service_category,
    SUM(fa.activity_actual) AS activity_actual,
    SUM(fa.activity_plan) AS activity_plan,
    SUM(fa.price_actual) AS price_actual,
    SUM(fa.price_plan) AS price_plan
FROM {{ ref('int_fa_lsacm_activity_plan_ytd') }} fa
--FROM DEV__MODELLING.CONTRACTING.INT_FA_LSACM_activity_plan_YTD fa
LEFT JOIN latestmonth l on fa.provider_code = l.provider_code
group by all
order by provider_shorthand, spec_comm_flag, commissioned_service