{{
    config(
        materialized='table',
        tags=['adult_imms'])
}}


--Historical View of Vaccination events. Keeping the same logic as current vaccinations but not linked to Currently eligible population. 

WITH IMMS_CODE_OBS as (
    SELECT
        dem.PERSON_ID, 
        clut.VACCINE_ORDER,
        clut.vaccine,
        clut.vaccine_id,
        clut.CODECLUSTERID,
        clut.code,
        clut.dose_match,
        clut.schedule_dose as dose_number,
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
    --look for events across the historical population by age at event in OBS table rather than age of person.
        
     )
--FIND all vaccination events coded as drugs by joining the mapped concept codes in the medication orders table (~100,000 rows)
,IMMS_CODE_MED as (
SELECT DISTINCT 
        dem.PERSON_ID,
        clut.VACCINE_ORDER,
        clut.vaccine,
        clut.vaccine_id,
        clut.CODECLUSTERID,
        clut.code,
        clut.dose_match,
        clut.schedule_dose as dose_number,
        DATE(m.clinical_effective_date) as EVENT_DATE,
        --m."age_at_event" is from EMIS. It either rounds years up or down
         m.age_at_event AS AGE_AT_EVENT
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
----Define Vaccination Events by codecluster - do not bother with out of schedule. 
,IMM_ADM as ( 
     SELECT distinct
        clut.PERSON_ID,
        clut.AGE_AT_EVENT,
	    clut.VACCINE_ORDER,
        clut.VACCINE_ID,
        clut.DOSE_NUMBER,
        clut.EVENT_DATE,
            CASE 
            WHEN clut.codeclusterid LIKE '%_ADM' THEN 'Administration'
            WHEN clut.codeclusterid LIKE '%_DRUG' THEN 'Administration'
            WHEN clut.codeclusterid LIKE '%_CONTRA' THEN 'Contraindicated'
            WHEN clut.codeclusterid LIKE '%_DEC' THEN 'Declined'
            ELSE NULL
        END AS EVENT_TYPE
        FROM VACCS_COMBINED clut 
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
    AGE_AT_EVENT,
    VACCINE_ORDER,
    VACCINE_ID,
    DOSE_NUMBER,
    EVENT_DATE,
    EVENT_TYPE,
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
 FROM IMM_ADM
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY person_id, vaccine_id, event_date
        ORDER BY priority
    ) = 1
-- keep only rows that are not beaten by a future better priority
    AND priority = best_future_priority
)
--IDENTIFY DUPLICATE ROWS WHERE SAME CODE CAN BE USED FOR DIFFERENT DOSES
,IMM_ADM_DOSE_DEDUP as (
	SELECT 
	PERSON_ID,
    AGE_AT_EVENT,
    VACCINE_ORDER,
	VACCINE_ID,
	DOSE_NUMBER,
    EVENT_TYPE,
	EVENT_DATE,
	--ROW_NUMBER() OVER (PARTITION BY PERSON_ID, VACCINE_ID ORDER BY EVENT_DATE ASC) AS row_num 
    --keep event-type in.
   ROW_NUMBER() OVER (PARTITION BY PERSON_ID, VACCINE_ID, EVENT_TYPE ORDER BY EVENT_DATE ASC) AS row_num
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


--SELECT FINAL VACCINATIONS DATASET DE-DUPLICATION BY EVENT_DATE and DOSE 
 SELECT 
	PERSON_ID,
	AGE_AT_EVENT,
    VACCINE_ORDER,
	VACCINE_ID,
	DOSE_NUMBER,
    EVENT_TYPE,
	EVENT_DATE
FROM ADMIN_CONTRA_CONFLICT
where rownum_contra = 1