{{
    config(
        materialized='table',
        tags=['adult_imms'])
}}

--FIND all vaccination events coded as observations by joining the mapped concept codes in the observation table. 
WITH IMMS_CODE_OBS as (
    SELECT
        dem.PERSON_ID,
        clut.VACCINE_ORDER,
        clut.vaccine,
        clut.vaccine_id,
        clut.CODECLUSTERID,
        clut.code,
        clut.dose_match,
        DATE(o.clinical_effective_date) as EVENT_DATE,
        --o."age_at_event" is from EMIS. It either rounds years up or down
        o.age_at_event AS AGE_AT_EVENT
    FROM {{ ref('stg_olids_observation') }} o
    --FROM MODELLING.DBT_STAGING.STG_OLIDS_OBSERVATION o
   LEFT JOIN  {{ ref('int_patient_person_unique') }} pp on pp.PATIENT_ID = o.patient_id
    --LEFT JOIN  MODELLING.OLIDS_PERSON_ATTRIBUTES.INT_PATIENT_PERSON_UNIQUE pp on pp.PATIENT_ID = o.patient_id
    INNER JOIN {{ ref('dim_person_demographics') }} dem ON pp.PERSON_ID = dem.PERSON_ID
     --INNER JOIN REPORTING.OLIDS_PERSON_DEMOGRAPHICS.DIM_PERSON_DEMOGRAPHICS dem ON pp.PERSON_ID = dem.PERSON_ID
    --Join mapped_concept_code to the IMMS_CODE_DOSEMATCH 
    JOIN {{ ref('int_adult_imms_code_dose') }} clut on o.mapped_concept_code  = clut.CODE 
    --JOIN MODELLING.OLIDS_PROGRAMME.INT_ADULT_IMMS_CODE_DOSE clut on o.mapped_concept_code  = clut.CODE 
        WHERE o.clinical_effective_date <= CURRENT_DATE
     )
--FIND all vaccination events coded as drugs by joining the mapped concept codes in the medication orders table.
,IMMS_CODE_MED as (
SELECT DISTINCT 
        dem.PERSON_ID,
        clut.VACCINE_ORDER,
        clut.vaccine,
        clut.vaccine_id,
        clut.CODECLUSTERID,
        clut.code,
        clut.dose_match,
        DATE(m.clinical_effective_date) as EVENT_DATE,
        --m."age_at_event" is from EMIS. It either rounds years up or down
         m.age_at_event AS AGE_AT_EVENT,
    FROM {{ ref('stg_olids_medication_order') }} m
    --FROM MODELLING.DBT_STAGING.STG_OLIDS_MEDICATION_ORDER m
    LEFT JOIN  {{ ref('int_patient_person_unique') }} pp on pp.PATIENT_ID = m.patient_id
    --LEFT JOIN  MODELLING.OLIDS_PERSON_ATTRIBUTES.INT_PATIENT_PERSON_UNIQUE pp on pp.PATIENT_ID = m.patient_id
    INNER JOIN {{ ref('dim_person_demographics') }} dem ON pp.PERSON_ID = dem.PERSON_ID
    --INNER JOIN REPORTING.OLIDS_PERSON_DEMOGRAPHICS.DIM_PERSON_DEMOGRAPHICS dem ON pp.PERSON_ID = dem.PERSON_ID
    JOIN {{ ref('int_adult_imms_code_dose') }} clut on m.mapped_concept_code  = clut.CODE
    --JOIN MODELLING.OLIDS_PROGRAMME.INT_ADULT_IMMS_CODE_DOSE clut on m.mapped_concept_code  = clut.CODE
    WHERE m.clinical_effective_date <= CURRENT_DATE
)
--UNION OBSERVATIONS AND MEDICATIONS. Only add drug events if they do not already exist as an admin code
,VACCS_COMBINED AS (
select o.*
from IMMS_CODE_OBS o
UNION ALL
select m.*
from IMMS_CODE_MED m
where not exists (
    select 1
   from IMMS_CODE_OBS o
    where o.person_id = m.person_id
     and o.vaccine_id = m.vaccine_id
    and o.event_date = m.event_date
    and o.dose_match = m.dose_match
)
)
--MATCH RECORDED IMMS EVENTS TO ELIGIBLE POPULATION AND DEFINE 'OUT OF SCHEDULE'
,IMM_ADM_ELIG as (  
     SELECT distinct
        el.PERSON_ID,
        el.BIRTH_DATE_APPROX,
        el.AGE,
        el.AGE_BAND_5Y,
        el.IS_CARE_HOME_RESIDENT,
        el.IS_IMMUNOSUPPRESSED,
        el.IN_PPV_CLINICAL_RISK_GROUP,
        el.IS_PREGNANT,
        el.TURN_65_AFTER_SEP_2023,
        el.AGE_DAYS_APPROX,
        clut.AGE_AT_EVENT,
	    clut.VACCINE_ORDER,
        el.VACCINE_ID,
        el.VACCINE_NAME,
        el.DOSE_NUMBER,
        el.ELIGIBLE_FROM_DATE,
        el.ELIGIBLE_TO_DATE,
        el.MAXIMUM_AGE_DAYS,
       clut.EVENT_DATE,
            CASE 
            WHEN clut.codeclusterid LIKE '%_ADM' THEN 'Administration'
            WHEN clut.codeclusterid LIKE '%_DRUG' THEN 'Administration'
            WHEN clut.codeclusterid LIKE '%_CONTRA' THEN 'Contraindicated'
            WHEN clut.codeclusterid LIKE '%_DEC' THEN 'Declined'
            ELSE NULL
        END AS EVENT_TYPE,
         -- Determine if the event was out of schedule for any of the events not just (clut.administered_cluster_id,clut.drug_cluster_id )
        CASE 
            WHEN datediff(day,el.BIRTH_DATE_APPROX,clut.event_date) > el.ELIGIBLE_AGE_TO_DAYS + 15 THEN TRUE 
            WHEN datediff(day,el.BIRTH_DATE_APPROX,clut.event_date) < el.ELIGIBLE_AGE_FROM_DAYS - 15 THEN TRUE 
           ELSE FALSE 
        END AS OUT_OF_SCHEDULE      
    FROM {{ ref('int_adult_imms_currently_eligible') }} el 
    --FROM MODELLING.OLIDS_PROGRAMME.INT_ADULT_IMMS_CURRENTLY_ELIGIBLE el
    INNER JOIN VACCS_COMBINED clut on clut.PERSON_ID = el.PERSON_ID
    AND el.DOSE_NUMBER = clut.DOSE_MATCH 
    and el.VACCINE_NAME = clut.VACCINE
    and el.VACCINE_ID = clut.VACCINE_ID
       --imms date must be greater than 1st day of the birth month BIRTH_DATE_APPROX
    and clut.EVENT_DATE > DATE_TRUNC('MONTH',el.BIRTH_DATE_APPROX)
   )
--IDENTIFY ROWS WHERE THERE ARE CONFLICTS AMONG ADMIN, DECLINED AND CONTRAINDICATED
/*
Admin after anything → Admin wins
Contra after Declined → Contra wins
Multiple Declineds → last one wins
Same-day conflicts → still handled by ROW_NUMBER()
*/
,IMM_ADM_DECLINED_CONFLICT as (
SELECT 
    PERSON_ID,
    BIRTH_DATE_APPROX,
    IS_CARE_HOME_RESIDENT,
    IS_IMMUNOSUPPRESSED,
    IN_PPV_CLINICAL_RISK_GROUP,
    IS_PREGNANT,
    TURN_65_AFTER_SEP_2023,
    AGE,
    AGE_BAND_5Y,
    AGE_DAYS_APPROX,
    AGE_AT_EVENT,
    VACCINE_ORDER,
    VACCINE_ID,
    VACCINE_NAME,
    DOSE_NUMBER,
    ELIGIBLE_FROM_DATE,
    ELIGIBLE_TO_DATE,
    MAXIMUM_AGE_DAYS,
    EVENT_DATE,
    EVENT_TYPE,
    OUT_OF_SCHEDULE,
    CASE
        WHEN event_type LIKE 'Admin%' THEN 1
        WHEN event_type LIKE 'Contra%' THEN 2
        WHEN event_type LIKE 'Declined%' THEN 3
        ELSE 99
    END AS priority,
    MIN(
        CASE
            WHEN event_type LIKE 'Admin%' THEN 1
            WHEN event_type LIKE 'Contra%' THEN 2
            WHEN event_type LIKE 'Declined%' THEN 3
            ELSE 99
        END
    ) OVER (
        PARTITION BY person_id, vaccine_id
        ORDER BY event_date
        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    ) AS best_future_priority
 FROM IMM_ADM_ELIG
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY person_id, vaccine_id, event_date
        ORDER BY priority
    ) = 1
-- keep only rows that are not beaten by a future better priority
    AND priority = best_future_priority
)
--IDENTIFY DUPLICATE ROWS WHERE SAME CODE CAN BE USED FOR DIFFERENT DOSES (SHINGLES)
,IMM_ADM_DOSE_DEDUP as (
SELECT 
	PERSON_ID,
    BIRTH_DATE_APPROX,
    IS_CARE_HOME_RESIDENT,
    IS_IMMUNOSUPPRESSED,
    IN_PPV_CLINICAL_RISK_GROUP,
    IS_PREGNANT,
    TURN_65_AFTER_SEP_2023,
    AGE,
    AGE_BAND_5Y,
    AGE_DAYS_APPROX,
    AGE_AT_EVENT,
    VACCINE_ORDER,
    VACCINE_ID,
    VACCINE_NAME,
    DOSE_NUMBER,
    ELIGIBLE_FROM_DATE,
    ELIGIBLE_TO_DATE,
    MAXIMUM_AGE_DAYS,
    EVENT_DATE,
    EVENT_TYPE,
    OUT_OF_SCHEDULE,
    --do not partiton by event_type 
       ROW_NUMBER() OVER (PARTITION BY PERSON_ID, VACCINE_ID ORDER BY EVENT_DATE ASC) AS row_num
    FROM IMM_ADM_DECLINED_CONFLICT   
      ) 
--in some cases someone has been vaccinated and then had a subsequent contraindicated code added. Choose earlier Admin
,ADMIN_CONTRA_CONFLICT as (
select *,
ROW_NUMBER() OVER (
            PARTITION BY person_id, vaccine_id
            ORDER BY
                CASE
                    WHEN EVENT_TYPE LIKE 'Admin%' THEN 1
                    WHEN EVENT_TYPE = 'Contraindicated' THEN 2
                    ELSE 3
                END,
                event_date DESC
        ) AS rownum_contra
FROM IMM_ADM_DOSE_DEDUP
WHERE 
--deduplicate where codes are non dose specific 
(dose_number = 1 AND row_num = 1)
OR (dose_number = 2 AND row_num = 2) 
)
--ADD VACCINATION STATUS FOR EVENTS.
select *
,CASE 
---PPV-------------------------------------------------------------------WHEN VACCINATIONS EXIST
--PPV_1 routine for people aged 65+
WHEN IN_PPV_CLINICAL_RISK_GROUP = FALSE AND VACCINE_ID in ('PPV_1B') THEN 'Not applicable'
--PPV_1B for immunosuppressed and other conditions 
WHEN IN_PPV_CLINICAL_RISK_GROUP AND VACCINE_ID in ('PPV_1') THEN 'Not applicable'
--SHINGLES---------------------------------------------------------------WHEN VACCINATIONS EXIST
-- Shingles turns 65 after 2023 SHING_1 and SHING_2
WHEN TURN_65_AFTER_SEP_2023 AND IS_IMMUNOSUPPRESSED = FALSE AND VACCINE_ID in ('SHING_1B','SHING_2B', 'SHING_1C', 'SHING_2C') THEN 'Not applicable'
-- Shingles routine catch up 70-79 SHING_1B and SHING_2B
WHEN TURN_65_AFTER_SEP_2023 = FALSE AND IS_IMMUNOSUPPRESSED = FALSE AND VACCINE_ID in ('SHING_1','SHING_2', 'SHING_1C', 'SHING_2C') THEN 'Not applicable'
--Shingles for 18+ immunosuppressed SHING_1C and SHING_2C
WHEN IS_IMMUNOSUPPRESSED AND TURN_65_AFTER_SEP_2023 = FALSE AND VACCINE_ID in ('SHING_1','SHING_2','SHING_1B','SHING_2B') THEN 'Not applicable'
-- Shingles turns 65 after 2023 AND IS_IMMUNOSUPPRESSED SHING_1 and SHING_2 OR SHING_1C and SHING_2C
WHEN IS_IMMUNOSUPPRESSED AND TURN_65_AFTER_SEP_2023 AND VACCINE_ID in ('SHING_1B','SHING_2B') THEN 'Not applicable'
--RSV---------------------------------------------------------------------WHEN VACCINATIONS EXIST
--Routine aged 75+ RSV_1
WHEN IS_CARE_HOME_RESIDENT = FALSE AND IS_PREGNANT = FALSE AND VACCINE_ID in ('RSV_1B','RSV_1C') THEN 'Not applicable'
--1st April 2026 introduce RSV_1B for older adult care home residents 
WHEN IS_CARE_HOME_RESIDENT AND IS_PREGNANT = FALSE AND VACCINE_ID in ('RSV_1','RSV_1C') THEN 'Not applicable'
--RSV for pregnant women RSV_1C
WHEN IS_PREGNANT AND IS_CARE_HOME_RESIDENT = FALSE AND VACCINE_ID in ('RSV_1','RSV_1B') THEN 'Not applicable'
WHEN IS_PREGNANT AND IS_CARE_HOME_RESIDENT AND VACCINE_ID in ('RSV_1') THEN 'Not applicable'
---ALL vaccs----------------------------------------------------------------------
WHEN EVENT_DATE IS NULL AND ELIGIBLE_FROM_DATE >= CURRENT_DATE() THEN 'Not due yet'
WHEN EVENT_DATE IS NULL AND ELIGIBLE_FROM_DATE < CURRENT_DATE() AND AGE_DAYS_APPROX < maximum_age_days THEN 'Overdue'
WHEN EVENT_TYPE LIKE ('Admin%') AND OUT_OF_SCHEDULE = FALSE THEN 'Completed'  
WHEN EVENT_TYPE LIKE ('Admin%') AND OUT_OF_SCHEDULE THEN 'OutofSchedule'
WHEN EVENT_DATE IS NULL AND AGE_DAYS_APPROX > maximum_age_days THEN 'No longer eligible'
WHEN EVENT_TYPE = 'Declined' THEN 'Declined'  
WHEN EVENT_TYPE = 'Contraindicated' THEN 'Contraindicated' 
END as VACCINATION_STATUS
--,ROW_NUMBER() OVER (PARTITION BY PERSON_ID, VACCINE_ID ORDER BY EVENT_DATE DESC) as rownum
from ADMIN_CONTRA_CONFLICT
where rownum_contra = 1