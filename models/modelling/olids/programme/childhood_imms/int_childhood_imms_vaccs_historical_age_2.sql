{{
    config(
        materialized='table',
        tags=['childhood_imms'])
}}

WITH HIST2YRBASE AS (
select distinct
p.PERSON_ID
,p.practice_name
,p.age
,p.BIRTH_DATE_APPROX
,p.BORN_SEP_2022_FLAG
,p.BORN_JUL_2024_FLAG
,p.BORN_JAN_2025_FLAG
,p.FIRST_BDAY
,v.VACCINE_ID
,v.VACCINE_NAME
,v.VACCINE_ORDER
,v.EVENT_DATE
,v.EVENT_TYPE
,v.AGE_AT_EVENT
FROM {{ ref('int_childhood_imms_historical_population') }} p
LEFT JOIN {{ ref('int_childhood_imms_vaccination_events_historical') }} v using (PERSON_ID)
--restrict by AGE not by VACCINE as for currently aged 2 - otherwise base population is not correct
where p.age = 2 
)
-- Creating CTE for 6in1 (dose 1,2,3) born before 1st July 2024
,SIXIN1 AS (
       SELECT 
        v1.PERSON_ID, 
        v1.EVENT_DATE AS sixin1_first_date, 
       	v1.AGE_AT_EVENT as sixin1_first_event_age,
        v2.EVENT_DATE AS sixin1_second_date,
      	v2.AGE_AT_EVENT as sixin1_second_event_age,
        v3.EVENT_DATE AS sixin1_third_date,
      	v3.AGE_AT_EVENT as sixin1_third_event_age,
--HELPER COLUMN to check number of months between DOB and vaccination date to check not 24 months
    ROUND(MONTHS_BETWEEN(v3.EVENT_DATE, v1.BIRTH_DATE_APPROX)) AS sixin1_third_event_age_mths
    FROM HIST2YRBASE v1
    LEFT JOIN HIST2YRBASE v2 ON v1.PERSON_ID = v2.PERSON_ID AND v2.VACCINE_ID = '6IN1_2' AND v2.EVENT_TYPE LIKE 'Admin%'
    LEFT JOIN HIST2YRBASE v3 ON v1.PERSON_ID = v3.PERSON_ID AND v3.VACCINE_ID = '6IN1_3' AND v3.EVENT_TYPE LIKE 'Admin%'
    WHERE v1.VACCINE_ID = '6IN1_1'  AND v1.EVENT_TYPE LIKE 'Admin%'
    AND (v1.BORN_JUL_2024_FLAG = FALSE AND v1.BORN_JAN_2025_FLAG = FALSE)
)
-- Creating CTE for 6in1 (dose 1,2,3,4) born on or after 1st July 2024 OR BORN on or after 1st January 2025 - 4th dose added at 18 months to replace HibMenC 
,SIXIN1B AS (
       SELECT 
        v1.PERSON_ID, 
        v1.EVENT_DATE AS sixin1_first_date, 
        v1.AGE_AT_EVENT as sixin1_first_event_age,
        v2.EVENT_DATE AS sixin1_second_date,
        v2.AGE_AT_EVENT as sixin1_second_event_age,
        v3.EVENT_DATE AS sixin1_third_date,
	v3.AGE_AT_EVENT as sixin1_third_event_age,
--HELPER COLUMN to check number of months between DOB and vaccination date to check not 24 months
    ROUND(MONTHS_BETWEEN(v3.EVENT_DATE, v1.BIRTH_DATE_APPROX)) AS sixin1_third_event_age_mths,
       v4.EVENT_DATE AS sixin1_fourth_date,
      v4.AGE_AT_EVENT as sixin1_fourth_event_age,
--HELPER COLUMN to check number of months between DOB and 4th vaccination date to check not 24 months
    ROUND(MONTHS_BETWEEN(v4.EVENT_DATE, v1.BIRTH_DATE_APPROX)) AS sixin1_fourth_event_age_mths
    FROM HIST2YRBASE v1
    LEFT JOIN HIST2YRBASE v2 ON v1.PERSON_ID = v2.PERSON_ID AND v2.VACCINE_ID = '6IN1_2' AND v2.EVENT_TYPE LIKE 'Admin%'
    LEFT JOIN HIST2YRBASE v3 ON v1.PERSON_ID = v3.PERSON_ID AND v3.VACCINE_ID = '6IN1_3' AND v3.EVENT_TYPE LIKE 'Admin%'
    LEFT JOIN HIST2YRBASE v4 ON v1.PERSON_ID = v4.PERSON_ID AND v4.VACCINE_ID = '6IN1_4' AND v4.EVENT_TYPE LIKE 'Admin%'
    WHERE v1.VACCINE_ID = '6IN1_1' AND v1.EVENT_TYPE LIKE 'Admin%'
    AND (v1.BORN_JUL_2024_FLAG OR v1.BORN_JAN_2025_FLAG)
)
-- Creating CTE for HibMenC (dose 1) applies to those born before 1st July 2024
,HIBMENC AS (
    SELECT 
         v1.PERSON_ID, 
         v1.EVENT_DATE AS hibmc_first_date,
        v1.AGE_AT_EVENT as hibmc_first_event_age,
--HELPER COLUMN to check number of months between DOB and vaccination date to check not 24 months
    ROUND(MONTHS_BETWEEN(v1.EVENT_DATE, v1.BIRTH_DATE_APPROX)) AS hibmc_event_age_mths,
         FROM HIST2YRBASE v1
        WHERE v1.VACCINE_ID = 'HIBMENC_1' and v1.EVENT_TYPE LIKE 'Admin%'
        AND (v1.BORN_JUL_2024_FLAG = FALSE AND v1.BORN_JAN_2025_FLAG = FALSE)
)  
-- Creating CTE for MMR (dose 1) when born on or after 1st Sep 2022 and before 1st January 2025 receive single dose of MMR by the age of 2 
,MMR1 AS (
    SELECT 
        v1.PERSON_ID, 
        v1.EVENT_DATE AS mmr_first_date,
        v1.AGE_AT_EVENT as mmr_first_event_age,
--HELPER column number of months between first vaccination date and approx first bday. If negative then EVENT is early and not valid
        ROUND(MONTHS_BETWEEN(v1.EVENT_DATE, v1.FIRST_BDAY)) AS mmr_first_bday_mths,
--HELPER COLUMN to check number of months between DOB and vaccination date to check not > 24 months
        ROUND(MONTHS_BETWEEN(v1.EVENT_DATE, v1.BIRTH_DATE_APPROX)) AS mmr_first_event_age_mths
        FROM HIST2YRBASE v1
        WHERE v1.VACCINE_ID = 'MMR_1' and v1.EVENT_TYPE LIKE 'Admin%'
        AND v1.BORN_JAN_2025_FLAG = FALSE
) 
-- Creating new CTE for MMRV (dose 1) when born on or after 1st July 2024 they receive MMRV as MMR Dose 2 at 18 months
,MMRV1B AS (
    SELECT 
        v1.PERSON_ID, 
        v1.EVENT_DATE AS mmrv_first_date,
        v1.AGE_AT_EVENT as mmrv_first_event_age,
--HELPER column number of months between first EVENT date and approx first bday. If negative then EVENT is early and not valid
        ROUND(MONTHS_BETWEEN(v1.EVENT_DATE, v1.FIRST_BDAY)) AS mmrv_first_bday_mths,
--HELPER COLUMN to check number of months between DOB and EVENT date is not >24 months
        ROUND(MONTHS_BETWEEN(v1.EVENT_DATE, v1.BIRTH_DATE_APPROX)) AS mmrv_first_event_age_mths
          FROM HIST2YRBASE v1
          WHERE v1.VACCINE_ID = 'MMRV_1B' AND v1.EVENT_TYPE LIKE 'Admin%'
          AND v1.BORN_JUL_2024_FLAG 
) 
-- Creating new CTE children born on or after 1st January 2025 - receive two doses of MMRV by the age of two. First at age 1 and 2nd dose at 18 months
,MMRV1 AS (
    SELECT 
        v1.PERSON_ID, 
        v1.EVENT_DATE AS mmrv_first_date,
        v1.AGE_AT_EVENT as mmrv_first_event_age,
--HELPER column number of months between first EVENT date and approx first bday. If negative then EVENT is early and not valid
        ROUND(MONTHS_BETWEEN(v1.EVENT_DATE, v1.FIRST_BDAY)) AS mmrv_first_bday_mths,
--HELPER COLUMN to check number of months between DOB and EVENT date is not >24 months
        ROUND(MONTHS_BETWEEN(v1.EVENT_DATE, v1.BIRTH_DATE_APPROX)) AS mmrv_first_event_age_mths,
         v2.EVENT_DATE AS mmrv_second_date,
         v2.AGE_AT_EVENT as mmrv_second_event_age,
--HELPER COLUMN to check number of months between DOB and second EVENT date is not >24 months
    ROUND(MONTHS_BETWEEN(v2.EVENT_DATE, v1.BIRTH_DATE_APPROX)) AS mmrv_second_event_age_mths
          FROM HIST2YRBASE v1
          LEFT JOIN HIST2YRBASE v2 ON v1.PERSON_ID = v2.PERSON_ID AND v2.VACCINE_ID = 'MMRV_2' AND v2.EVENT_TYPE LIKE 'Admin%'
          WHERE v1.VACCINE_ID = 'MMRV_1' AND v1.EVENT_TYPE LIKE 'Admin%'
        AND v1.BORN_JAN_2025_FLAG 
)
-- Creating CTE for MenB (dose 1 and 2 and booster) APPLIES TO ALL COHORTS
,MENB AS (
    SELECT 
    v1.PERSON_ID, 
    v1.EVENT_DATE AS menb_first_date, 
   v1.AGE_AT_EVENT as menb_first_event_age,
    v2.EVENT_DATE AS menb_second_date,
   v2.AGE_AT_EVENT as menb_second_event_age,
    v3.EVENT_DATE AS menb_third_date,
v3.AGE_AT_EVENT as menb_third_event_age,
--HELPER COLUMN to check number of months between DOB and vaccination date to check not 24 months
    ROUND(MONTHS_BETWEEN(v3.EVENT_DATE, v1.BIRTH_DATE_APPROX)) AS menb_third_event_age_mths
    FROM HIST2YRBASE v1
    LEFT JOIN HIST2YRBASE v2 
    ON v1.PERSON_ID = v2.PERSON_ID AND v2.VACCINE_ID = 'MENB_2' AND v2.EVENT_TYPE LIKE 'Admin%'
    LEFT JOIN HIST2YRBASE v3 ON v1.PERSON_ID = v3.PERSON_ID AND v3.VACCINE_ID = 'MENB_3' AND v3.EVENT_TYPE LIKE 'Admin%'
    WHERE v1.VACCINE_ID = 'MENB_1' and v1.EVENT_TYPE LIKE 'Admin%'
)
-- Creating CTE for PCV (dose 1 and 2) APPLIES TO ALL COHORTS
,PCV AS (
    SELECT 
        v1.PERSON_ID,  
        v1.EVENT_DATE AS pcv_first_date,
        v2.EVENT_DATE AS pcv_second_date,
        v2.AGE_AT_EVENT as pcv_second_event_age,
--calculate HELPER column number of months between second vaccination date and approx first bday. If it's a negative number than the vaccination is early and not valid
ROUND(MONTHS_BETWEEN(v2.EVENT_DATE, v1.FIRST_BDAY)) AS pcv_second_first_bday_mths,
         FROM HIST2YRBASE v1
         LEFT JOIN HIST2YRBASE v2 
         ON v1.PERSON_ID = v2.PERSON_ID AND v2.VACCINE_ID = 'PCV_2' AND v2.EVENT_TYPE LIKE 'Admin%'
         WHERE v1.VACCINE_ID = 'PCV_1' AND v1.EVENT_TYPE LIKE 'Admin%'
         AND v1.BIRTH_DATE_APPROX >= '2020-01-16'
         )
,COMBINED AS (
SELECT DISTINCT
v.PERSON_ID 
,v.PRACTICE_NAME
,v.AGE
---------------------------------------------------------------------------------------------------------------------------------------
--FOR THOSE BORN BEFORE 1st JULY 2024
--6-IN-1 3X doses must be less than 2 on event date
,CASE WHEN s.sixin1_first_event_age <= 2 AND s.sixin1_second_event_age <= 2 AND s.sixin1_third_event_age <= 2 AND s.sixin1_third_event_age_mths <= 24 THEN 1 
     ELSE 0 end as sixin1_comp_by_2
--HIBMENC must be less than or equal to 2 on event date and check that event age in mths is <= 24 mths 
,CASE WHEN h.hibmc_first_event_age <= 2 AND h.hibmc_event_age_mths <= 24 THEN 1 ELSE 0  END AS hibmc_comp_by_2
 --MMR dose age at event years must be at least 1 but <3 and first dose must not be before FIRST_BDAY (ie mmr1_first_bday_mths is negative)
,CASE WHEN mr.mmr_first_event_age BETWEEN 1 AND 2 AND mr.mmr_first_bday_mths >= 0 AND mmr_first_event_age_mths <=24 THEN 1 ELSE 0 END AS mmr1_comp_by_2
-----------------------------------------------------------------------------------------------------------------------------------------
--FOR THOSE BORN ON OR AFTER 1st JULY 2024
    --6-IN-1 4x doses. AGE AT EVENT must be less than or equal to 2 and check that event age in mths is BETWEEN 18 AND 24 mths.Fourth dose at 18mths replaces HIBMenC
,CASE WHEN s1.sixin1_first_event_age <= 2 AND s1.sixin1_second_event_age <= 2 AND s1.sixin1_third_event_age <= 2 AND s1.sixin1_fourth_event_age <= 2
    AND s1.sixin1_fourth_event_age_mths between 18 AND 24 THEN 1 ELSE 0 end as sixin1_4_comp_by_2   
    --MMRV dose 1 replaces MMR Dose 2 AGE AT EVENT years must be at least 1 but <3 and first dose must not be before FIRST_BDAY (ie mmr1_first_bday_mths is negative)
-- ,CASE WHEN mv1b.mmrv_first_event_age BETWEEN 1 AND 2 AND mv1b.mmrv_first_bday_mths >= 0 AND mv1b.mmrv_first_event_age_mths between 18 AND 24 THEN 1 
--     ELSE 0 END AS mmrv1_comp_by_2 - SEE BELOW FOR NEW MMRV1 LOGIC FOR THOSE BORN ON OR AFTER 1ST JANUARY 2025
---------------------------------------------------------------------------------------------------------------------------------------------
--FOR THOSE BORN ON OR AFTER 1st JANUARY 2025
    --MMRV Dose 1 at 12mths and MMRV Dose 2 at 18 mths and before or on second birthday OR on or after their first birthday and before or on second birthday 
    --MMRV Doses 1 and 2 Evaluate whether or not MMRV (dose 2) has been completed between 18 mths and 2 years and after first dose 
,CASE WHEN mv1.mmrv_first_event_age BETWEEN 1 AND 2 AND mv1.mmrv_first_bday_mths >= 0 AND mv1.mmrv_first_event_age_mths <=24 THEN 1 
    WHEN mv1b.mmrv_first_event_age BETWEEN 1 AND 2 AND mv1b.mmrv_first_bday_mths >= 0 AND mv1b.mmrv_first_event_age_mths between 18 AND 24 THEN 1
        ELSE 0 END AS mmrv1_comp_by_2
,CASE WHEN mv1.mmrv_first_event_age BETWEEN 1 AND 2 AND mv1.mmrv_first_bday_mths >= 0 AND mv1.mmrv_second_date >= mv1.mmrv_first_date 
    AND mv1.mmrv_second_event_age <=2 AND mv1.mmrv_second_event_age_mths between 18 AND 24 THEN 1 
    ELSE 0 END AS mmrv2_comp_by_2
---------------------------------------------------------------------------------------------------------------------------------------------------------------
--APPLIES TO ALL
--all menB must be less than 2 on event date
,CASE WHEN m.menb_third_event_age <= 2 AND m.menb_third_event_age_mths <=24 THEN 1 ELSE 0 END AS menb_comp_by_2
--PCV 2nd dose age at event years must be at least 1 but <3 and 2nd dose must not be before FIRST_BDAY (ie pcv_second_first_bday_mths is negative)
,CASE WHEN p.pcv_second_event_age BETWEEN 1 AND 2 AND pcv_second_first_bday_mths >= 0 
AND FLOOR(DATEDIFF('day', p.pcv_first_date, p.pcv_second_date) / 7) >= 4 THEN 1
  WHEN p.pcv_first_date IS NULL AND p.pcv_second_event_age BETWEEN 1 AND 2 AND pcv_second_first_bday_mths >= 0 THEN 1
     ELSE 0 END AS pcv_comp_by_2   
FROM HIST2YRBASE v  
left join SIXIN1 s using (PERSON_ID) 
left join SIXIN1B s1 using (PERSON_ID)
left join HIBMENC h using (PERSON_ID)   
left join MMR1 mr using (PERSON_ID) 
left join MMRV1B mv1b using (PERSON_ID)
left join MMRV1 mv1 using (PERSON_ID)
left join MENB m using (PERSON_ID) 
left join PCV p using (PERSON_ID) 
)
,FINAL_PREJOIN AS (
SELECT 
person_id
,practice_name
,age
,sixin1_comp_by_2
,sixin1_4_comp_by_2
,hibmc_comp_by_2
,mmr1_comp_by_2
,mmrv1_comp_by_2
,mmrv2_comp_by_2
,menb_comp_by_2
,pcv_comp_by_2
--Define conditions for when all vaccs are completed
,CASE WHEN 
    --FOR THOSE BORN BEFORE 1st JULY 2024
	(sixin1_comp_by_2 = 1 AND hibmc_comp_by_2 = 1 AND mmr1_comp_by_2 = 1 AND menb_comp_by_2 = 1 AND pcv_comp_by_2 = 1)
    --FOR THOSE BORN ON OR AFTER 1st JULY 2024
    OR (sixin1_4_comp_by_2 = 1 AND mmrv1_comp_by_2 = 1 AND menb_comp_by_2 = 1 AND pcv_comp_by_2 = 1)
    --FOR THOSE BORN ON OR AFTER 1st JANUARY 2025
    OR (sixin1_4_comp_by_2 = 1 AND mmrv1_comp_by_2 = 1 AND mmrv2_comp_by_2 = 1 AND menb_comp_by_2 = 1 AND pcv_comp_by_2 = 1)
    THEN 1 ELSE 0 END AS all_comp_by_2 
FROM COMBINED
)
--add back in demographics and person months
select 
p.ANALYSIS_MONTH
,p.fiscal_year_label
,v.PERSON_ID
,v.PRACTICE_NAME
,p.practice_code
,v.AGE
,p.ethnicity_category
,p.ethcat_order
,p.imd_quintile
,p.imdquintile_order
,p.RESIDENTIAL_BOROUGH
,p.residential_neighbourhood
,p.ward_name
,v.sixin1_comp_by_2
,v.sixin1_4_comp_by_2
,v.hibmc_comp_by_2
,v.mmr1_comp_by_2
,v.mmrv1_comp_by_2
,v.mmrv2_comp_by_2
,v.menb_comp_by_2
,v.pcv_comp_by_2
,v.all_comp_by_2
FROM {{ ref('int_childhood_imms_historical_population') }} p 
LEFT JOIN FINAL_PREJOIN v
 on p.person_id = v.person_id and p.age= v.age and p.practice_name = v.practice_name
 where p.age = 2

