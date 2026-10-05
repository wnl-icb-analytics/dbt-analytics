{{
    config(
        materialized='table',
        tags=['adult_imms'],
        cluster_by=['person_id'])
}}
--THIS TABLE CAPTURES RSV FOR THE COHORTS AGE 75+ ROUTINE, CARE HOME RESIDENTS aged 50+, CURRENTLY PREGNANT 
--AND NEW FOR SEPTEMBER 2026 THOSE WITH CLINICAL RISK AGED 65-74 (IMMUNOSUPPRESSED OR CHRONIC RESPIRATORY DISEASE)
WITH
-- All eligible people 
eligible AS (
    SELECT 
        person_id
        ,age
       ,TRUE as eligible
        ,IS_CARE_HOME_RESIDENT
        ,IS_PREGNANT
        ,IN_RSV_CLINICAL_RISK_GROUP
    FROM {{ ref('int_adult_imms_current_population') }}
    --FROM MODELLING.OLIDS_PROGRAMME.INT_ADULT_IMMS_CURRENT_POPULATION
    where age >= 75 or (IS_CARE_HOME_RESIDENT AND AGE >=50) OR IS_PREGNANT OR (IN_RSV_CLINICAL_RISK_GROUP and AGE BETWEEN 65 AND 74)
)
-- RSV SINGLE DOSE
,rsv as (
select *
from 
    (
--Single Dose given
    SELECT 
        person_id
        ,'RSV' As campaign
        ,vaccination_date
       ,'VACCINATION_ADMINISTERED' as vaccination_status
    FROM {{ ref('int_adult_imms_rsv_vaccination_given') }}
    --FROM DEV__MODELLING.OLIDS_PROGRAMME.INT_ADULT_IMMS_RSV_VACCINATION_GIVEN
    where vaccination_status is not null
UNION
--Single Dose declined
    SELECT 
        person_id
         ,'RSV' As campaign
        ,vaccination_date
       ,'VACCINATION_DECLINED' as vaccination_status
    FROM {{ ref('int_adult_imms_rsv_vaccination_declined') }}
    --FROM DEV__MODELLING.OLIDS_PROGRAMME.INT_ADULT_IMMS_RSV_VACCINATION_DECLINED
   where vaccination_status is not null
) a
order by 1,3
) 

select e.*
,CASE WHEN p.campaign is null THEN 'RSV' ELSE p.campaign END AS campaign 
,p.vaccination_date
,CASE
WHEN p.vaccination_status is null THEN 'NO_VACCINATION_RECORD' ELSE p.vaccination_status END AS vaccination_status
from eligible e
LEFT JOIN rsv p using (person_id)