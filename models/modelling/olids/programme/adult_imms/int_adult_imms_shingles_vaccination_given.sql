{{
    config(
        materialized='table',
        tags=['adult_imms'],
        cluster_by=['person_id'])
}}
--people aged 65+ who are eligible who have received Shingles vaccine. Two doses needed. Latest vaccination recorded
--people eligible either turned 65 after Sept 2023 (SHING_1 & SHING_2) or are currently 70-79 for the Catch Up programme (SHING_1B & SHING_2B)
--as of September 2025 people aged 18+ who are immunosuppressed (SHING_1C & SHING_2C).
--Shingles 1
   with shing1 as (
   SELECT 
       PERSON_ID 
        ,AGE AS CURRENT_AGE
        ,TURN_65_AFTER_SEP_2023
        ,IS_IMMUNOSUPPRESSED
        ,VACCINE_ID as VACCINE_ID_FIRST
        ,VACCINATION_DATE AS shing_first_date
        ,VACCINATION_STATUS AS shing_first_status
        ,AGE_AT_EVENT as  shing_first_event_age
         --This row prioritises 'Completed' over 'OutofSchedule' where IS_IMMUNOSUPPRESSED and TURN_65_AFTER_SEP_2023 are BOTH TRUE 
        ,ROW_NUMBER() OVER (
            PARTITION BY person_id
            ORDER BY
                CASE
                    WHEN VACCINATION_STATUS = 'Completed' THEN 1
                    WHEN VACCINATION_STATUS = 'OutofSchedule' THEN 2
                    ELSE 3
                END,
                VACCINATION_DATE DESC) as rownum
    FROM {{ ref('int_adult_imms_vaccination_status_current') }}  v1
    --FROM MODELLING.OLIDS_PROGRAMME.INT_ADULT_IMMS_VACCINATION_STATUS_CURRENT 
    WHERE VACCINE_ID LIKE ('SHING_1%') and VACCINATION_STATUS in ('Completed', 'OutofSchedule')  
    QUALIFY rownum = 1
    )
    --Second Shingles
     ,shing2 as (
    SELECT 
       PERSON_ID 
        ,AGE AS CURRENT_AGE
        ,TURN_65_AFTER_SEP_2023
        ,IS_IMMUNOSUPPRESSED
        ,VACCINE_ID as VACCINE_ID_SECOND
        ,VACCINATION_DATE AS shing_second_date
        ,VACCINATION_STATUS AS shing_second_status
        ,AGE_AT_EVENT as  shing_second_event_age
         --This row prioritises 'Completed' over 'OutofSchedule' where IS_IMMUNOSUPPRESSED and TURN_65_AFTER_SEP_2023 are BOTH TRUE 
        ,ROW_NUMBER() OVER (
            PARTITION BY person_id
            ORDER BY
                CASE
                    WHEN VACCINATION_STATUS = 'Completed' THEN 1
                    WHEN VACCINATION_STATUS = 'OutofSchedule' THEN 2
                    ELSE 3
                END,
                VACCINATION_DATE DESC) as rownum
    FROM {{ ref('int_adult_imms_vaccination_status_current') }}  v1
    --FROM DEV__MODELLING.OLIDS_PROGRAMME.INT_ADULT_IMMS_VACCINATION_STATUS_CURRENT 
    WHERE VACCINE_ID in ('SHING_2','SHING_2B','SHING_2C') and VACCINATION_STATUS in ('Completed', 'OutofSchedule')  
    QUALIFY rownum = 1
    ) 
  
   SELECT 
        v1.PERSON_ID 
        ,v1.CURRENT_AGE
        ,v1.TURN_65_AFTER_SEP_2023
        ,v1.IS_IMMUNOSUPPRESSED
        ,v1.VACCINE_ID_FIRST
        ,v1.shing_first_date
        ,v1.shing_first_status
        ,v1.shing_first_event_age
        ,v2.VACCINE_ID_SECOND
        ,v2.shing_second_date
        ,v2.shing_second_status
        ,v2.shing_second_event_age
     FROM SHING1 v1
    LEFT JOIN SHING2 v2
    ON v1.PERSON_ID = v2.PERSON_ID AND v1.SHING_FIRST_DATE < v2.SHING_SECOND_DATE 