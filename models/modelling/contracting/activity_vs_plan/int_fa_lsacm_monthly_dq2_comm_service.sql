{{
    config(
        materialized='view',
        tags=['fa_checks'])
}}
--list activity vs plan by commissioner service category code, service code and spec_comm_flag for the latest LSACM monthly submission used for DQ check 2.
SELECT
    provider_shorthand,
    spec_comm_flag,
    CONCAT(commissioned_service_category_code, ' - ', cssc_name) as commissioned_service,
    CONCAT(service_code, ' - ', INITCAP(LOWER(service_code_desc))) as service_code,
    SUM(activity_actual) AS activity_actual,
    SUM(activity_plan) AS activity_plan,
    SUM(price_actual) AS price_actual,
    SUM(price_plan) AS price_plan
FROM {{ ref('int_fa_lsacm_activity_plan_ytd') }}
--FROM MODELLING.CONTRACTING.INT_FA_LSACM_activity_plan_YTD
group by all
order by provider_shorthand, spec_comm_flag, commissioned_service