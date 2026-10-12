--get all latest data for plan activity checking every month by joining to latest data meta_file_id
{{
    config(
        materialized='table',
        tags=['fa_checks'])
}}
WITH WNL_REPORTING_NAMES AS (
    SELECT DISTINCT
        provider_code,
        provider_shorthand,
        provider_name
    FROM {{ ref('stg_reference_wnl_reporting_names') }}
    --FROM STAGING.REFERENCE.STG_REFERENCE_WNL_REPORTING_NAMES
)
--USE STAGED LSACM DATA JOINING TO LATEST SUBMISSION DATA BY META_FILE_ID
SELECT
ACM.META_FILE_ID,
ACM.DV_PROVIDER_CODE as PROVIDER_CODE,
ORG.PROVIDER_NAME,
org.provider_shorthand,
 ACM.DV_FINANCIAL_MONTH as FINANCIAL_MONTH,
 LS.SUBMITTED_ROW_COUNT,
 LS.SUBMISSION_STATUS,
 LS.FILE_VERSION,
 ACM.COMMISSIONER_CODE,
 ACM.POINT_OF_DELIVERY_CODE,
 ACM.LOCAL_POINT_OF_DELIVERY_DESCRIPTION,
 ACM.SERVICE_CODE,
 ACM.COMMISSIONED_SERVICE_CATEGORY_CODE,
CASE
        WHEN ACM.COMMISSIONER_CODE IN ('13Q','13R') 
            THEN 'Retained'
        WHEN ACM.SERVICE_CODE = 'NCBPSXXX' THEN
             CASE WHEN COMMISSIONED_SERVICE_CATEGORY_CODE = '21' THEN 'Retained'
                  WHEN COMMISSIONED_SERVICE_CATEGORY_CODE = '25' THEN 'Delegated'
                  ELSE 'Retained' END                  
        WHEN ACM.SERVICE_CODE IN ('CCGPS08S_EXC','99999999','NULL') 
             OR ACM.SERVICE_CODE LIKE '%99999%' 
             OR ACM.SERVICE_CODE IS NULL 
            THEN 'ICB-commissioned'
        WHEN ACM.DV_FINANCIAL_YEAR <= '202526' THEN 
            CASE 
                WHEN SER.ICB_DELEGATION_25_26 IN ('RED','BLUE','NOT APPLICABLE') 
                    THEN 'Retained'
                WHEN SER.ICB_DELEGATION_25_26 = 'GREEN' 
                    THEN 'Delegated'
                ELSE 'Invalid Service Code'
            END
        WHEN ACM.DV_FINANCIAL_YEAR = '202627' THEN 
            CASE 
                WHEN SER.ICB_DELEGATION_26_27 IN ('RED','BLUE','NOT APPLICABLE') 
                    THEN 'Retained'
                WHEN SER.ICB_DELEGATION_26_27 = 'GREEN' 
                    THEN 'Delegated'
                ELSE 'Invalid Service Code'
            END
        ELSE 'Invalid Service Code' 
    END AS SPEC_COMM_FLAG
,IFF(COMMISSIONER_CODE IN ('07X','07M','08H','08D','07R','93C','07P','07W','07Y','08C','08E','08G','08Y','09A','W2U3Z','Y05','T04','Z9B2Z'),'WNL','Other') COMM_GROUP,
ROUND(ACM.DV_ACTUAL_ACTIVITY,0) as ACTIVITY_ACTUAL,
ROUND(ACM.DV_PLANNED_ACTIVITY,0) as ACTIVITY_PLAN,
ACM.DV_ACTUAL_PRICE AS PRICE_ACTUAL,
ACM.DV_PLANNED_PRICE AS PRICE_PLAN
FROM {{ ref('stg_lsacm') }} ACM
--FROM STAGING.SLAM.STG_LSACM ACM
--join on latest file submitted
INNER JOIN {{ ref('int_fa_lsacm_monthly_sub_ytd_latest') }} ls using (meta_file_id)
--INNER JOIN MODELLING.CONTRACTING.INT_FA_LSACM_MONTHLY_SUB_YTD_LATEST ls on ls.meta_file_id = acm.meta_file_id 
LEFT JOIN {{ ref('stg_reference_nhse_service_codes_by_year_v38') }} ser on acm.service_code = ser.service_code
--LEFT JOIN STAGING.REFERENCE.STG_REFERENCE_NHSE_SERVICE_CODES_BY_YEAR_V38 ser ON ACM.SERVICE_CODE = SER.SERVICE_CODE
LEFT JOIN WNL_REPORTING_NAMES org on acm.dv_provider_code = org.provider_code