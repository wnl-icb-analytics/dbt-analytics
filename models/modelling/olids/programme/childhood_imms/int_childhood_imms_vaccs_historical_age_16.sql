{{
    config(
        materialized='table',
        tags=['childhood_imms'])
}}

WITH HIST16YRBASE AS (
select distinct
p.PERSON_ID
,p.practice_name
,p.age
,p.BIRTH_DATE_APPROX
,p.FIRST_BDAY
,p.TWELFTH_BDAY
,p.THIRTEENTH_BDAY
,v.VACCINE_ID
,v.VACCINE_NAME
,v.VACCINE_ORDER
,v.EVENT_DATE
,v.EVENT_TYPE
,v.AGE_AT_EVENT
FROM {{ ref('int_childhood_imms_historical_population') }} p
LEFT JOIN {{ ref('int_childhood_imms_vaccination_events_historical') }} v using (PERSON_ID)
--restrict by AGE not by VACCINE as for currently aged 16 - otherwise base population is not correct
where p.age = 16

)

-- Creating CTE for 6in1 (dose 1,2,3) where 1 row is per patient AS NUMERATOR
,SIXIN1 AS (
       SELECT 
        v1.PERSON_ID, 
        v1.EVENT_DATE AS sixin1_first_date, 
       	v1.AGE_AT_EVENT as sixin1_first_event_age,
        v2.EVENT_DATE AS sixin1_second_date,
      	v2.AGE_AT_EVENT as sixin1_second_event_age,
        v3.EVENT_DATE AS sixin1_third_date,
      	v3.AGE_AT_EVENT as sixin1_third_event_age,
    --HELPER COLUMN to check number of months between DOB and vaccination date to check not > 192 months (16 years)
    ROUND(MONTHS_BETWEEN(v3.EVENT_DATE, v1.BIRTH_DATE_APPROX)) AS sixin1_third_event_age_mths
    FROM HIST16YRBASE v1
    LEFT JOIN HIST16YRBASE v2 ON v1.PERSON_ID = v2.PERSON_ID AND v2.VACCINE_ID = '6IN1_2' AND v2.EVENT_TYPE LIKE 'Admin%'
    LEFT JOIN HIST16YRBASE v3 ON v1.PERSON_ID = v3.PERSON_ID AND v3.VACCINE_ID = '6IN1_3' AND v3.EVENT_TYPE LIKE 'Admin%'
    WHERE v1.VACCINE_ID = '6IN1_1'  AND v1.EVENT_TYPE LIKE 'Admin%'
)

-- Creating CTE for 4-in-1 (dose 1) where 1 row is per patient AS NUMERATOR
,FOURIN1 AS (
    SELECT 
        v1.PERSON_ID, 
        v1.EVENT_DATE AS fourin1_first_date,
        v1.AGE_AT_EVENT as fourin1_first_event_age,
--HELPER COLUMN to check number of months between DOB and vaccination date is not > 192 months (16 years)
    ROUND(MONTHS_BETWEEN(v1.EVENT_DATE, v1.BIRTH_DATE_APPROX)) AS fourin1_event_age_mths
         FROM HIST16YRBASE v1
        WHERE v1.VACCINE_ID = '4IN1_1' and v1.EVENT_TYPE LIKE 'Admin%'
)  
 -- Creating CTE for 3-in-1 (dose 1) where 1 row is per patient AS NUMERATOR
,THREEIN1 AS (
        SELECT 
        v1.PERSON_ID
        ,v1.EVENT_DATE AS threein1_first_date
		,v1.AGE_AT_EVENT as threein1_first_event_age
--HELPER column number of months between vaccination date and approx 13th bday. If it's a negative number than the vaccination is early and not valid
        ,ROUND(MONTHS_BETWEEN(v1.EVENT_DATE, v1.THIRTEENTH_BDAY)) AS threein1_thirteenth_bday_mths   
--HELPER COLUMN to check number of months between DOB and vaccination date is not > 192 months (16 years)
        ,ROUND(MONTHS_BETWEEN(v1.EVENT_DATE, v1.BIRTH_DATE_APPROX)) AS threein1_event_age_mths
        FROM HIST16YRBASE v1
        WHERE v1.VACCINE_ID = '3IN1_1' AND v1.EVENT_TYPE LIKE 'Admin%'
)

-- Creating CTE for MMR (dose 1 & Dose 2) where 1 row is per patient AS NUMERATOR
,MMR AS (
        SELECT 
        v1.PERSON_ID, 
        v1.EVENT_DATE AS mmr_first_date,
        v1.AGE_AT_EVENT as mmr_first_event_age,
--HELPER column number of months between first vaccination date and approx first bday. If it's a negative number than the vaccination is early and not valid
        ROUND(MONTHS_BETWEEN(v1.EVENT_DATE, v1.FIRST_BDAY)) AS mmr_first_bday_mths,   
 --HELPER COLUMN to check number of months between DOB and second vaccination date is not > 192 months (16 years)
    ROUND(MONTHS_BETWEEN(v2.EVENT_DATE, v1.BIRTH_DATE_APPROX)) AS mmr_second_event_age_mths,
         v2.EVENT_DATE AS mmr_second_date,
         v2.AGE_AT_EVENT as mmr_second_event_age,
         FROM HIST16YRBASE v1
        LEFT JOIN HIST16YRBASE v2 ON v1.PERSON_ID = v2.PERSON_ID AND v2.VACCINE_ID = 'MMR_2'  AND v2.EVENT_TYPE LIKE 'Admin%'
        WHERE v1.VACCINE_ID = 'MMR_1' and v1.EVENT_TYPE LIKE 'Admin%'
) 
-- Creating CTE for HPV (dose 1) where 1 row is per patient AS NUMERATOR. 
--HPV as a SINGLE DOSE 1 >= 12th bday & <= 16th bday OR Dose 1 is null & Dose 2 >=twelfth_bday & <= 16th bday
,HPV AS (
    SELECT 
        v1.PERSON_ID 
        ,v1.EVENT_DATE AS hpv_first_date
		,v1.AGE_AT_EVENT as hpv_first_event_age
--HELPER column number of months between first vaccination date and approx twelfth bday. If it's a negative number than the vaccination is early and not valid
       ,ROUND(MONTHS_BETWEEN(v1.EVENT_DATE, v1.TWELFTH_BDAY)) AS hpv_first_twelfth_bday_mths  
--HELPER COLUMN to check number of months between DOB and first vaccination date is not > 192 months (16 years)
        ,ROUND(MONTHS_BETWEEN(v1.EVENT_DATE, v1.BIRTH_DATE_APPROX)) AS hpv_first_event_age_mths
        ,v2.EVENT_DATE AS hpv_second_date
        ,v2.AGE_AT_EVENT as hpv_second_event_age
--HELPER column number of months between second vaccination date and approx twelfth bday. If it's a negative number than the vaccination is early and not valid
       ,ROUND(MONTHS_BETWEEN(v2.EVENT_DATE, v1.TWELFTH_BDAY)) AS hpv_second_twelfth_bday_mths
--HELPER COLUMN to check number of months between DOB and second vaccination date is not > 192 months (16 years) 
        ,ROUND(MONTHS_BETWEEN(v2.EVENT_DATE, v1.BIRTH_DATE_APPROX)) AS hpv_second_event_age_mths
        FROM HIST16YRBASE v1
        LEFT JOIN HIST16YRBASE v2 ON v1.PERSON_ID = v2.PERSON_ID AND v2.VACCINE_ID = 'HPV_2' AND v2.EVENT_TYPE LIKE 'Admin%'
        WHERE v1.VACCINE_ID = 'HPV_1' 
        --AND v1.EVENT_TYPE LIKE 'Admin%' allow for missing first dose
)

-- Creating CTE for MenACWY  (dose 1) where 1 row is per patient AS NUMERATOR 
,MENACWY AS (
    SELECT 
    v1.PERSON_ID
    ,v1.EVENT_DATE AS menacwy_first_date
    ,v1.AGE_AT_EVENT as menacwy_first_event_age
--HELPER column number of months between vaccination date and approx 13th bday. If it's a negative number than the vaccination is early and not valid
        ,ROUND(MONTHS_BETWEEN(v1.EVENT_DATE, v1.THIRTEENTH_BDAY)) AS menacwy_thirteenth_bday_mths   
--HELPER COLUMN to check number of months between DOB and vaccination date is not > 192 months 
         ,ROUND(MONTHS_BETWEEN(v1.EVENT_DATE, v1.BIRTH_DATE_APPROX)) AS menacwy_event_age_mths
    FROM HIST16YRBASE v1
    WHERE v1.VACCINE_ID = 'MENACWY_1' AND v1.EVENT_TYPE LIKE 'Admin%'
)

,COMBINED AS (
SELECT DISTINCT
v.PERSON_ID 
,v.PRACTICE_NAME
,v.AGE
--all 3 doses must be less than 2 on event date
     ,CASE WHEN s.sixin1_first_event_age <= 16 AND s.sixin1_second_event_age <= 16 AND s.sixin1_third_event_age <= 16 AND s.sixin1_third_event_age_mths <= 192
     THEN 1 ELSE 0 end as sixin1_COMP_BY_16
--4-IN-1 ALIGN to HEI current logic - 1 doses anytime before or on sixteenth bday ie AGE AT EVENT must be less than or equal to 16 and check that event age in mths is <= 192 mths
    ,CASE WHEN f.fourin1_first_event_age <= 16 AND f.fourin1_event_age_mths <= 192 THEN 1 ELSE 0 END AS fourin1_COMP_BY_16
--HPV as a SINGLE DOSE 1 >= 12th bday (months between vacc date and 12th bday is >= 0) & age at event <= 16 and double check that hpv_first_event_age_mths <= 192
--OR Dose 1 is null & Dose 2 >=twelfth_bday & <= 16th bday
    ,CASE 
    WHEN hp.hpv_first_twelfth_bday_mths >=0  AND hp.hpv_first_event_age <= 16 AND hp.hpv_first_event_age_mths <=192 THEN 1
    WHEN hp.hpv_first_date IS NULL AND hp.hpv_second_twelfth_bday_mths >=0  AND hp.hpv_second_event_age <= 16 AND hp.hpv_second_event_age_mths <=192 THEN 1
    ELSE 0 END AS hpv_comp_by_16
--3-IN-1 Green Book teenage TETANUS booster from the age of 13 and 14 (school year 9 or 10)
     ,CASE WHEN t.threein1_thirteenth_bday_mths >= 0 AND t.threein1_first_event_age <= 16 AND t.threein1_event_age_mths <= 192 THEN 1 
    ELSE 0 END AS threein1_comp_by_16 
--MMR Doses 1 and 2 Evaluate whether or not MMR (dose 2) has been completed by the sixteenth birthday and after first dose 	and first dose is on or after first b-day
	,CASE WHEN mr.mmr_first_event_age BETWEEN 1 AND 2 AND mr.mmr_first_bday_mths >= 0  AND mr.mmr_second_date > mr.mmr_first_date AND mr.mmr_second_event_age <=16 
    AND mr.mmr_second_event_age_mths <=192 THEN 1 ELSE 0 END AS mmr_comp_by_16
 --MENACWY from the age of 13 and 14 (school year 9 or 10)
    ,CASE WHEN ma.menacwy_thirteenth_bday_mths >= 0 AND ma.menacwy_first_event_age <= 16 AND ma.menacwy_event_age_mths <= 192 THEN 1 ELSE 0 END AS menacwy_comp_by_16 
 
FROM HIST16YRBASE v  
left join SIXIN1 s using (PERSON_ID) 
left join FOURIN1 f using (PERSON_ID)  
left join THREEIN1 t using (PERSON_ID) 
left join MENACWY ma using (PERSON_ID)  
left join MMR mr using (PERSON_ID) 
left join HPV hp using (PERSON_ID)
)
,FINAL_PREJOIN AS (
SELECT 
person_id
,practice_name
,age
,SIXIN1_COMP_BY_16
,FOURIN1_COMP_BY_16
,HPV_COMP_BY_16
,THREEIN1_COMP_BY_16
,MMR_COMP_BY_16
,MENACWY_COMP_BY_16
,CASE 
	WHEN sixin1_comp_by_16 = 1 AND fourin1_comp_by_16 = 1 AND hpv_comp_by_16 = 1 AND threein1_comp_by_16 = 1 AND menacwy_comp_by_16 = 1 AND mmr_comp_by_16 = 1 	THEN 1
	ELSE 0 END AS all_COMP_BY_16
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
,v.SIXIN1_COMP_BY_16
,v.FOURIN1_COMP_BY_16
,v.HPV_COMP_BY_16
,v.THREEIN1_COMP_BY_16
,v.MMR_COMP_BY_16
,v.MENACWY_COMP_BY_16
,v.ALL_COMP_BY_16
FROM {{ ref('int_childhood_imms_historical_population') }} p 
LEFT JOIN FINAL_PREJOIN v
 on p.person_id = v.person_id and p.age= v.age and p.practice_name = v.practice_name
 where p.age = 16
order by 1,2