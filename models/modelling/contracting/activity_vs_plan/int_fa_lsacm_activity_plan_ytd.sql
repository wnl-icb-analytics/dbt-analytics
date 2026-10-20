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
o.organisation_name as commissioner_name,
acm.point_of_delivery_code,
acm.local_point_of_delivery_description,
--commissioned service category codes
acm.commissioned_service_category_code,
CASE 
WHEN acm.commissioned_service_category_code = 11 THEN 'Integrated Care Board' ELSE
REGEXP_REPLACE(
    INITCAP(LOWER(cs.short_description)),
    '\\bNhs\\b','NHS')  END AS cscc_name,
CASE WHEN acm.commissioned_service_category_code = 11 or cs.code is null THEN 'Invalid Code' ELSE 'Valid' END AS cscc_dq,
CASE 
WHEN acm.commissioned_service_category_code = 11 THEN 'ICB-commissioned' 
ELSE cs.funding_status END AS cscc_funding_status,
--service codes
acm.service_code,
REGEXP_REPLACE(
  REGEXP_REPLACE(
    REGEXP_REPLACE(
      INITCAP(LOWER(ser.service_code_description)),
      '\\bNhs\\b', 'NHS'),'\\bHiv\\b', 'HIV'),'\\bLvhc\\b', 'LVHC'
) AS service_code_desc,
INITCAP(LOWER(ser.icb_delegation_26_27)) as icb_delegation_26_27,
ser.current_funding_status as service_code_funding_status,
CASE 
WHEN acm.service_code IS NOT NULL and ser.service_code IS NULL THEN 'Invalid Code' 
WHEN RIGHT(acm.service_code, 1) REGEXP '[a-z]' THEN 'Code fix required'
WHEN acm.service_code IS NULL THEN NULL
ELSE 'Valid' END AS service_code_dq,
 --define SPEC_COMM_FLAG for reporting. icb_delegation_26_27 = 'BLUE' is locally commissioned so use the CSSC code for Funding Status
CASE
    WHEN acm.service_code = 'NULL' OR acm.service_code IS NULL THEN 'ICB-commissioned'
    WHEN acm.commissioner_code IN ('13Q','13R') THEN 'Retained' 
    WHEN UPPER(acm.service_code) LIKE '%XXX' THEN cs.funding_status 
    WHEN ser.service_code IS NOT NULL AND UPPER(acm.service_code) NOT LIKE '%XXX' AND ser.icb_delegation_26_27 <> 'BLUE' THEN ser.current_funding_status
    WHEN ser.service_code IS NOT NULL AND UPPER(acm.service_code) NOT LIKE '%XXX' AND ser.icb_delegation_26_27 = 'BLUE' THEN cs.funding_status
    ELSE 'Invalid Service Code' 
    END AS spec_comm_flag
,IFF(acm.commissioner_code IN ('07X','07M','08H','08D','07R','93C','07P','07W','07Y','08C','08E','08G','08Y','09A','W2U3Z','Y05','T04','Z9B2Z'),'WNL','Other') comm_group,
acm.dv_actual_activity as activity_actual,
acm.dv_planned_activity as activity_plan,
acm.dv_actual_price AS price_actual,
acm.dv_planned_price AS price_plan
FROM {{ ref('stg_lsacm') }} acm
--FROM STAGING.SLAM.STG_LSACM acm
--join on latest file submitted
INNER JOIN {{ ref('int_fa_lsacm_monthly_sub_ytd_latest') }} ls using (meta_file_id)
--INNER JOIN MODELLING.CONTRACTING.INT_FA_LSACM_MONTHLY_SUB_YTD_LATEST ls on ls.meta_file_id = acm.meta_file_id 
LEFT JOIN {{ ref('stg_dictionary_dbo_organisation') }} o on acm.commissioner_code = o.organisation_code
--LEFT JOIN STAGING.DICTIONARY.STG_DICTIONARY_DBO_ORGANISATION o on acm.commissioner_code = o.organisation_code
LEFT JOIN {{ ref('pss_service_code') }} ser on UPPER(acm.service_code) = ser.service_code
--LEFT JOIN REFERENCE.DATA_DICTIONARY.PSS_SERVICE_CODE ser on UPPER(acm.service_code) = ser.service_code
LEFT JOIN {{ ref('nhs_dd_commissioned_service_category') }} cs on acm.commissioned_service_category_code = cs.code
--LEFT JOIN REFERENCE.DATA_DICTIONARY.NHS_DD_COMMISSIONED_SERVICE_CATEGORY cs on acm.commissioned_service_category_code = cs.code
LEFT JOIN wnl_reporting_names org on acm.dv_provider_code = org.provider_code
)
select *
from latest_submissions
where comm_group = 'WNL'