{{
    config(
        materialized='table',
        tags=['adult_imms'],
        cluster_by=['person_id'])
}}
--people aged 75+ who are eligible who have received RSV vaccine. Latest vaccination recorded
--people eligible either turned 80 after Sept 2024  or are currently 75-79 for the Catch Up programme (RSV_1)
--women who are pregnant aged 12+ (RSV_1C)
--April 1st 2026 this is extended to people in care homes for older adults.(RSV_1B) proxy age 50+.
--September 1st 2026 this is extended to people in clinical risk groups (RSV_1D) proxy aged between 65 and 74
SELECT 
         PERSON_ID 
       ,AGE AS CURRENT_AGE
       ,GENDER
        ,IS_CARE_HOME_RESIDENT
        ,IN_RSV_CLINICAL_RISK_GROUP
        ,IS_PREGNANT
        ,VACCINE_ID 
        ,VACCINATION_DATE 
        ,VACCINATION_STATUS 
        ,AGE_AT_EVENT 
        --in case there are multiple records for declined on the same day for where IS_PREGNANT and IS_CARE_HOME_RESIDENT are both TRUE.
        ,ROW_NUMBER() OVER (PARTITION BY PERSON_ID ORDER BY VACCINATION_DATE ASC) AS rownum
    FROM {{ ref('int_adult_imms_vaccination_status_current') }}
    WHERE VACCINE_ID in ('RSV_1','RSV_1B','RSV_1C', 'RSV_1D') and VACCINATION_STATUS in ('Declined')  
    QUALIFY rownum = 1