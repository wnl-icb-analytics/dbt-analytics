--get all latest data for plan activity checking every month by joining to latest data meta_file_id
{{
    config(
        materialized='table',
        tags=['fa_checks'])
}}
WITH wnl_reporting_names AS (
    SELECT DISTINCT
        provider_code,
        provider_shorthand,
        provider_name
    FROM {{ ref('stg_reference_wnl_reporting_names') }}
    --FROM STAGING.REFERENCE.STG_REFERENCE_WNL_REPORTING_NAMES
)
--USE STAGED LSacm DATA JOINING TO LATEST SUBMISSION DATA BY META_FILE_ID
,latest_submissions AS (
SELECT
acm.meta_file_id,
acm.dv_provider_code as provider_code,
org.provider_name,
org.provider_shorthand,
acm.dv_financial_month as financial_month,
ls.submitted_row_count,
ls.submission_status,
ls.file_version,
acm.commissioner_code,
acm.point_of_delivery_code,
acm.local_point_of_delivery_description,
acm.service_code,
ser.npoc_category_and_crg as clinical_reference_group,
acm.commissioned_service_category_code,
CASE 
 WHEN acm.commissioned_service_category_code = 11 THEN 'INTEGRATED CARE BOARD' ELSE
 cs.short_description END as service_name,
CASE
        WHEN acm.commissioner_code IN ('13Q','13R') 
            THEN 'Retained'
        WHEN acm.service_code = 'NCBPSXXX' THEN
             CASE WHEN acm.commissioned_service_category_code = '21' THEN 'Retained'
                  WHEN acm.commissioned_service_category_code = '25' THEN 'Delegated'
                  ELSE 'Retained' END                  
        WHEN acm.service_code IN ('CCGPS08S_EXC','99999999','NULL') 
             OR acm.service_code LIKE '%99999%' 
             OR acm.SERVICE_CODE IS NULL 
            THEN 'ICB-commissioned'
        -- WHEN acm.dv_financial_year <= '202526' THEN 
        --     CASE 
        --         WHEN ser.icb_delegation_25_26 IN ('RED','BLUE','NOT APPLICABLE') 
        --             THEN 'Retained'
        --         WHEN ser.icb_delegation_25_26 = 'GREEN' 
        --             THEN 'Delegated'
        --         ELSE 'Invalid Service Code'
        --     END
        WHEN acm.dv_financial_year = '202627' THEN 
            CASE 
                WHEN ser.icb_delegation_26_27 IN ('RED','BLUE','NOT APPLICABLE') 
                    THEN 'Retained'
                WHEN ser.icb_delegation_26_27 = 'GREEN' 
                    THEN 'Delegated'
                ELSE 'Invalid Service Code'
            END
        ELSE 'Invalid Service Code' 
    END AS spec_comm_flag
,IFF(acm.commissioner_code IN ('07X','07M','08H','08D','07R','93C','07P','07W','07Y','08C','08E','08G','08Y','09A','W2U3Z','Y05','T04','Z9B2Z'),'WNL','Other') comm_group,
ROUND(acm.dv_actual_activity,0) as activity_actual,
ROUND(acm.dv_planned_activity,0) as activity_plan,
acm.dv_actual_price AS price_actual,
acm.dv_planned_price AS price_plan
FROM {{ ref('stg_lsacm') }} acm
--FROM STAGING.SLAM.STG_LSacm acm
--join on latest file submitted
INNER JOIN {{ ref('int_fa_lsacm_monthly_sub_ytd_latest') }} ls using (meta_file_id)
--INNER JOIN MODELLING.CONTRACTING.INT_FA_LSacm_MONTHLY_SUB_YTD_LATEST ls on ls.meta_file_id = acm.meta_file_id 
LEFT JOIN {{ ref('pss_service_code') }} ser on acm.service_code = ser.service_code
LEFT JOIN {{ ref('nhs_dd_commissioned_service_category') }} cs on acm.commissioned_service_category_code = cs.code
LEFT JOIN wnl_reporting_names org on acm.dv_provider_code = org.provider_code
)
select * 
from latest_submissions
where comm_group = 'WNL'