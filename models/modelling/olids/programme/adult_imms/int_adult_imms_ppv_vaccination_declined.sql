{{
    config(
        materialized='table',
        tags=['adult_imms'],
        cluster_by=['person_id'])
}}
--people aged 65+ who are eligible who have declined PPV vaccine. One dose only needed. Latest vaccination recorded
 SELECT 
        PERSON_ID
        ,AGE AS CURRENT_AGE
         ,IN_PPV_CLINICAL_RISK_GROUP
        ,VACCINE_ID
        ,VACCINATION_DATE 
        ,VACCINATION_STATUS 
	    ,AGE_AT_EVENT 
         --in case there are multiple records for declined on the same day.
        ,ROW_NUMBER() OVER (PARTITION BY PERSON_ID ORDER BY VACCINATION_DATE ASC) AS rownum
        FROM {{ ref('int_adult_imms_vaccination_status_current') }} 
    WHERE VACCINE_ID LIKE ('PPV_1%') AND VACCINATION_STATUS in ('Declined')
    QUALIFY rownum = 1