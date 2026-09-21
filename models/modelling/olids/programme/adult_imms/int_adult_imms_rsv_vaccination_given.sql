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
        ,IS_CARE_HOME_RESIDENT
        ,IS_PREGNANT
        ,IN_RSV_CLINICAL_RISK_GROUP
        ,VACCINE_ID 
        ,VACCINATION_DATE 
        ,VACCINATION_STATUS 
        ,AGE_AT_EVENT 
        --This row prioritises 'Completed' over 'OutofSchedule' where IS_PREGNANT and IS_CARE_HOME_RESIDENT are BOTH TRUE (rare cases where there is someone under 50 in a care home setting)
        ,ROW_NUMBER() OVER (
            PARTITION BY person_id
            ORDER BY
                CASE
                    WHEN VACCINATION_STATUS = 'Completed' THEN 1
                    WHEN VACCINATION_STATUS = 'OutofSchedule' THEN 2
                    ELSE 3
                END,
                VACCINATION_DATE DESC) as rownum
    FROM {{ ref('int_adult_imms_vaccination_status_current') }}
    --FROM MODELLING.OLIDS_PROGRAMME.INT_ADULT_IMMS_VACCINATION_STATUS_CURRENT
    WHERE VACCINE_ID in ('RSV_1','RSV_1B','RSV_1C', 'RSV_1D') and VACCINATION_STATUS in ('Completed', 'OutofSchedule') 
    QUALIFY rownum = 1  