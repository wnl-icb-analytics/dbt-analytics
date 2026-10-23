{{
    config(
        materialized='view',
        tags=['fa_checks'])
}}

with latestmonth as (
select provider_code, fy_month_submitted as financial_month
FROM {{ ref('int_fa_lsacm_monthly_sub_ytd_latest') }}
--FROM MODELLING.CONTRACTING.INT_FA_LSACM_MONTHLY_SUB_YTD_LATEST
)
--get POD activity and plan from LSACM latest submission.
,sub_plan as (
SELECT
        fa.provider_shorthand,
        fa.meta_file_id,
        l.financial_month as Latest_month,
        TO_NUMBER(fa.commissioned_service_category_code) as commissioned_service_category_code,
        CONCAT(fa.commissioned_service_category_code, ' - ', fa.cscc_name) as commissioned_service,
        fa.point_of_delivery_code,
        SUM(fa.activity_plan) AS activity_plan_sub,
        SUM(fa.price_plan) AS price_plan_sub
        FROM {{ ref('int_fa_lsacm_activity_plan_ytd') }} fa
        --FROM MODELLING.CONTRACTING.INT_FA_LSACM_activity_plan_YTD fa
        LEFT JOIN latestmonth l on fa.provider_code = l.provider_code
        GROUP BY ALL
)
--get POD activity and plan from IAP plans submitted 202627.
,iap_plan as (
SELECT
        iap.source_provider as provider_shorthand,
        iap.commissioned_service_category_code,
        iap.point_of_delivery_code,
        SUM(iap.contract_monitoring_planned_activity) AS activity_plan_iap,
        SUM(iap.contract_monitoring_planned_price) AS price_plan_iap
        FROM {{ ref('stg_reference_combined_contract_iap_202627') }} iap
        --FROM STAGING.REFERENCE.STG_REFERENCE_COMBINED_CONTRACT_IAP_202627 iap
        LEFT JOIN latestmonth l on iap.provider_code = l.provider_code 
        WHERE iap.commissioner_code IN ('07X','07M','08H','08D','07R','93C','07P','07W','07Y','08C','08E','08G','08Y','09A','W2U3Z','Y05','T04','Z9B2Z','QWL','QRV','QMJ')
        and iap.financial_month <= l.financial_month
        GROUP BY ALL
)
--FULL OUTER JOIN to account for any differences in POD between the 2 sources.
 SELECT
    COALESCE(s.provider_shorthand, i.provider_shorthand) AS provider_shorthand,
    s.latest_month,
     COALESCE(s.commissioned_service,'Not Assigned') AS commissioned_service,
    COALESCE(
        s.commissioned_service_category_code,
        i.commissioned_service_category_code
    ) AS commissioned_service_category_code,
    COALESCE(
        s.point_of_delivery_code,
        i.point_of_delivery_code
    ) AS point_of_delivery_code,
    CASE
    WHEN s.point_of_delivery_code IS NULL THEN 'Missing in SUB'
    WHEN i.point_of_delivery_code IS NULL THEN 'Missing in IAP'
    ELSE 'Matched' END AS pod_match,
    s.activity_plan_sub,
    i.activity_plan_iap,
    s.activity_plan_sub-i.activity_plan_iap as diff_activity,
    (s.activity_plan_sub - i.activity_plan_iap)/NULLIF(s.activity_plan_sub, 0)* 100 AS diff_activity_pct,
    s.price_plan_sub,
    i.price_plan_iap,
    s.price_plan_sub-i.price_plan_iap as diff_price,
    (s.price_plan_sub-i.price_plan_iap)/NULLIF(s.price_plan_sub, 0)* 100 AS diff_price_pct
FROM sub_plan s
FULL OUTER JOIN iap_plan i
    ON s.provider_shorthand = i.provider_shorthand
   AND s.commissioned_service_category_code = i.commissioned_service_category_code
   AND s.point_of_delivery_code = i.point_of_delivery_code
