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
acm.commissioned_service_category_code,
CASE 
 WHEN acm.commissioned_service_category_code = 11 THEN 'INTEGRATED CARE BOARD' ELSE
 INITCAP(LOWER(cs.short_description)) END as cssc_name,
cs.funding_status as cssc_funding_status,
acm.service_code,
INITCAP(LOWER(ser.npoc_category_and_crg)) as service_code_desc,
ser.icb_delegation_26_27,

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
LEFT JOIN {{ ref('pss_service_code') }} ser on acm.service_code = ser.service_code
LEFT JOIN {{ ref('nhs_dd_commissioned_service_category') }} cs on acm.commissioned_service_category_code = cs.code
LEFT JOIN wnl_reporting_names org on acm.dv_provider_code = org.provider_code
)
select 
meta_file_id,
provider_code,
provider_name,
provider_shorthand,
financial_month,
submitted_row_count,
submission_status,
file_version,
commissioner_code,
commissioner_name,
point_of_delivery_code,
local_point_of_delivery_description,
service_code,
npoc_category_and_crg as service_code_desc,
commissioned_service_category_code, 
CASE
    WHEN cssc_name = 'INTEGRATED CARE BOARD'
        THEN 'Integrated Care Board'
    WHEN cssc_name = 'NHS ENGLAND - SPECIALISED SERVICES'
        THEN 'NHS England - Specialised Services'
    WHEN cssc_name = 'DELEGATED SPECIALISED ACUTE SERVICES'
        THEN 'Delegated Specialised Acute Services'
    WHEN cssc_name = 'DELEGATED SPECIALISED MENTAL HEALTH SERVICES'
        THEN 'Delegated Specialised Mental Health Services'
    WHEN cssc_name = 'DELEGATED SECONDARY DENTISTRY'
        THEN 'Delegated Secondary Dentistry'
    WHEN cssc_name = 'NHS ENGLAND - ARMED FORCES'
        THEN 'NHS England - Armed Forces'
    WHEN cssc_name = 'NHS ENGLAND - HEALTH IN JUSTICE'
        THEN 'NHS England - Health in Justice'
    WHEN cssc_name = 'OTHER (NOT LISTED)'
        THEN 'Other'
    WHEN cssc_name = 'NOT KNOWN'
        THEN 'Not Known'
END AS cssc_name,
spec_comm_flag,
activity_actual,
activity_plan,
price_actual,
price_plan
from latest_submissions
where comm_group = 'WNL'