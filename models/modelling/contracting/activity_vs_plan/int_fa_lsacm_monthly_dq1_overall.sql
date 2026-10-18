{{
    config(
        materialized='view',
        tags=['fa_checks'])
}}
--First check of activity versus plan by commissioner code for WNL only. This is a high level check to see if the activity submitted is in line with the plan for the financial year and month.
--SOURCE STAGING.SLAM.STG_LSACM
SELECT
        fa.provider_shorthand,
        fa.commissioner_code,
        SUM(activity_actual) AS activity_actual,
        SUM(activity_plan) AS activity_plan,
        SUM(price_actual) AS price_actual,
        SUM(price_plan) AS price_plan
        FROM {{ ref('int_fa_lsacm_activity_plan_ytd') }} fa
        --FROM MODELLING.CONTRACTING.INT_FA_LSACM_ACTIVITY_PLAN_YTD
        GROUP BY ALL
        ORDER BY provider_shorthand, commissioner_code