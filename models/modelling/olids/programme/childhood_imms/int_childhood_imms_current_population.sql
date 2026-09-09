{{
    config(
        materialized='table',
        tags=['childhood_imms'],
        cluster_by=['person_id'])
}}
--define current population of children for childhood immunisations programme
with pop as (
SELECT DISTINCT
dem.PERSON_ID
,dem.BIRTH_DATE_APPROX
,dem.AGE 
,age.AGE_DAYS_APPROX
,CASE WHEN dem.BIRTH_DATE_APPROX >= '2022-09-01' AND dem.BIRTH_DATE_APPROX < '2024-07-01' THEN TRUE
ELSE FALSE END AS BORN_SEP_2022_FLAG
,CASE WHEN dem.BIRTH_DATE_APPROX >= '2024-07-01' AND dem.BIRTH_DATE_APPROX < '2025-01-01' THEN TRUE
ELSE FALSE END AS BORN_JUL_2024_FLAG
,CASE WHEN dem.BIRTH_DATE_APPROX >= '2025-01-01' THEN TRUE
ELSE FALSE END AS BORN_JAN_2025_FLAG
,dem.GENDER
,DATEADD(YEAR,1,dem.BIRTH_DATE_APPROX) as FIRST_BDAY
,DATEADD(YEAR,2,dem.BIRTH_DATE_APPROX) as SECOND_BDAY
,DATEADD(YEAR,3,dem.BIRTH_DATE_APPROX) as THIRD_BDAY
,DATEADD(YEAR,5,dem.BIRTH_DATE_APPROX) as FIFTH_BDAY
,DATEADD(YEAR,6,dem.BIRTH_DATE_APPROX) as SIXTH_BDAY
,DATEADD(YEAR,11,dem.BIRTH_DATE_APPROX) as ELEVENTH_BDAY
,DATEADD(YEAR,12,dem.BIRTH_DATE_APPROX) as TWELFTH_BDAY
,DATEADD(YEAR,13,dem.BIRTH_DATE_APPROX) as THIRTEENTH_BDAY
,DATEADD(YEAR,14,dem.BIRTH_DATE_APPROX) as FOURTEENTH_BDAY
,DATEADD(YEAR,15,dem.BIRTH_DATE_APPROX) as FIFTEENTH_BDAY
,DATEADD(YEAR,16,dem.BIRTH_DATE_APPROX) as SIXTEENTH_BDAY
,DATEADD(YEAR,17,dem.BIRTH_DATE_APPROX) as SEVENTEENTH_BDAY
,CASE
WHEN dem.ETHNICITY_CATEGORY = 'Not Recorded' THEN 'Unknown'
WHEN dem.ETHNICITY_GRANULAR in ('Black - E.African Asian', 'East African Asian') THEN 'Asian'
WHEN dem.ETHNICITY_GRANULAR = 'New Zealander' THEN 'White'
WHEN dem.ETHNICITY_GRANULAR = 'North African' THEN 'Other'
WHEN dem.ETHNICITY_GRANULAR in ('Not Recorded','Not stated','Not Stated','Recorded Not Known','Refused', 'Unknown') THEN 'Unknown'
ELSE dem.ETHNICITY_CATEGORY END AS ETHNICITY_CATEGORY
,CASE
WHEN dem.ETHNICITY_SUBCATEGORY in ('Not Recorded','Not stated','Not Stated','Recorded Not Known','Refused') THEN 'Unknown'
WHEN dem.ETHNICITY_GRANULAR = 'Indian' THEN 'Asian: Indian'
WHEN dem.ETHNICITY_GRANULAR in ('Black - E.African Asian', 'East African Asian') THEN 'Asian: Other Asian'
WHEN dem.ETHNICITY_GRANULAR = 'Hungarian Roma' THEN 'White: Traveller'
WHEN dem.ETHNICITY_GRANULAR = 'New Zealander' THEN 'White: Other White'
WHEN dem.ETHNICITY_GRANULAR = 'North African' THEN 'Other: Other'
WHEN dem.ETHNICITY_GRANULAR = 'Not Stated' THEN 'Unknown'
WHEN dem.ETHNICITY_GRANULAR in ('Not Recorded','Not stated','Not Stated','Recorded Not Known','Refused', 'Unknown')
THEN 'Unknown'
ELSE dem.ETHNICITY_SUBCATEGORY END AS ETHNICITY_SUBCATEGORY
,CASE 
WHEN dem.ETHNICITY_GRANULAR = 'Not stated' THEN 'Not Stated'
WHEN dem.ETHNICITY_GRANULAR = 'MENA' THEN 'Middle East and North African'
WHEN dem.ETHNICITY_GRANULAR in ('Recorded Not Known', 'Refused', 'Not stated', 'Not Recorded','Not Stated') THEN 'Unknown'
WHEN dem.ETHNICITY_GRANULAR = 'Black - E.African Asian' THEN 'East African Asian'
WHEN dem.ETHNICITY_GRANULAR = 'Cornish' THEN 'English'
WHEN dem.ETHNICITY_GRANULAR in ('Gypsy or Irish Traveller','Gypsy','Irish Traveller') THEN 'Gypsy or Irish Traveller'
WHEN dem.ETHNICITY_GRANULAR in ('Albanian/Serbian', 'Serbian') THEN 'Albanian or Serbian'
ELSE dem.ETHNICITY_GRANULAR END AS ETHNICITY_GRANULAR
--switch to IMD25
,COALESCE(dem.IMD_QUINTILE_25, 'Unknown') AS IMD_QUINTILE
,CASE 
WHEN dem.IMD_QUINTILE_25 = 'Most Deprived' THEN 1
WHEN dem.IMD_QUINTILE_25 = 'Second Most Deprived' THEN 2
WHEN dem.IMD_QUINTILE_25 = 'Third Most Deprived' THEN 3
WHEN dem.IMD_QUINTILE_25 = 'Second Least Deprived' THEN 4
WHEN dem.IMD_QUINTILE_25 = 'Least Deprived' THEN 5
ELSE 6 END AS IMDQUINTILE_ORDER
,dem.IMD_DECILE_25 AS IMD_DECILE
,CASE 
WHEN dem.MAIN_LANGUAGE = 'Pushto' THEN 'Pashto' 
WHEN dem.MAIN_LANGUAGE = 'Gujerati' THEN 'Gujarati'
WHEN dem.MAIN_LANGUAGE ILIKE '%sign language%' THEN 'Sign language'
WHEN dem.MAIN_LANGUAGE = 'Norwegian Bokmål' THEN 'Norwegian'
WHEN dem.MAIN_LANGUAGE = 'Not Recorded' THEN 'Unknown'
ELSE dem.MAIN_LANGUAGE END AS MAIN_LANGUAGE
,dem.BOROUGH_REGISTERED AS PRACTICE_BOROUGH 
,dem.NEIGHBOURHOOD_REGISTERED AS PRACTICE_NEIGHBOURHOOD
,dem.PCN_NAME AS PRIMARY_CARE_NETWORK
,dem.PRACTICE_NAME AS GP_NAME
,dem.PRACTICE_CODE
,COALESCE(dem.local_authority_name,'Unknown') as RESIDENTIAL_BOROUGH
,COALESCE(dem.NEIGHBOURHOOD_RESIDENT,'Unknown') as RESIDENTIAL_NEIGHBOURHOOD
,case
    -- all NCL Boroughs
    when dem.local_authority_code in ('E09000003', 'E09000007', 'E09000010', 'E09000014', 'E09000019') then 'NCL'
    -- all NWL Boroughs
    when dem.local_authority_code in ('E09000005','E09000009','E09000013','E09000015','E09000017','E09000018','E09000020','E09000033') then 'NWL'
    --all NEL Boroughs
    when dem.local_authority_code in ('E09000002','E09000001','E09000012','E09000016','E09000025','E09000026','E09000030','E09000031') then 'NEL'
    when dem.local_authority_code like 'E09%' and dem.local_authority_code not in ('E09000003', 'E09000007', 'E09000010', 'E09000014', 'E09000019','E09000005', 
        'E09000009','E09000013','E09000015','E09000017','E09000018','E09000020','E09000033','E09000002','E09000001','E09000012',
        'E09000016','E09000025','E09000026','E09000030','E09000031') then 'Other London'
    when dem.local_authority_code is null then 'Unknown'
    else 'Outside London'
    end as residential_loc
,dem.WARD_CODE
,dem.WARD_NAME
,dem.LSOA_CODE_21
,CASE WHEN l.PERSON_ID IS NULL  THEN 'No' ELSE 'Yes' END AS LAC_FLAG
,dem.is_active
,dem.is_deceased
FROM {{ ref('dim_person_demographics') }} dem
LEFT JOIN {{ ref('dim_person_age') }} age on age.PERSON_ID = dem.PERSON_ID
LEFT JOIN {{ ref('dim_looked_after_child') }}  l on l.PERSON_ID = dem.PERSON_ID
-- LEFT JOIN {{ ref('stg_reference_lsoa21_ward25_lad25') }} la on la.LSOA21_CD = dem.LSOA_CODE_21
WHERE dem.is_active  
AND dem.IS_DECEASED = FALSE
AND dem.age < 20
-- AND ICB_CODE = 'QMJ'
)
select *, 
CASE 
WHEN ETHNICITY_CATEGORY = 'Asian' THEN 1
WHEN ETHNICITY_CATEGORY = 'Black' THEN 2
WHEN ETHNICITY_CATEGORY = 'Mixed' THEN 3
WHEN ETHNICITY_CATEGORY = 'Other' THEN 4
WHEN ETHNICITY_CATEGORY = 'White' THEN 5
WHEN ETHNICITY_CATEGORY = 'Unknown' THEN 6
WHEN ETHNICITY_CATEGORY = 'Not Recorded' THEN 6
END AS ETHCAT_ORDER,
CASE 
WHEN ETHNICITY_SUBCATEGORY = 'Asian: Bangladeshi' THEN 1
WHEN ETHNICITY_SUBCATEGORY = 'Asian: Chinese' THEN 2
WHEN ETHNICITY_SUBCATEGORY = 'Asian: Indian' THEN 3
WHEN ETHNICITY_SUBCATEGORY = 'Asian: Pakistani' THEN 4
WHEN ETHNICITY_SUBCATEGORY = 'Asian: Other Asian' THEN 5
WHEN ETHNICITY_SUBCATEGORY = 'Black: African' THEN 6
WHEN ETHNICITY_SUBCATEGORY = 'Black: Caribbean' THEN 7
WHEN ETHNICITY_SUBCATEGORY = 'Black: Other Black' THEN 8
WHEN ETHNICITY_SUBCATEGORY = 'Mixed: White and Asian' THEN 9
WHEN ETHNICITY_SUBCATEGORY = 'Mixed: White and Black African' THEN 10
WHEN ETHNICITY_SUBCATEGORY = 'Mixed: White and Black Caribbean' THEN 11
WHEN ETHNICITY_SUBCATEGORY = 'Mixed: Other Mixed' THEN 12
WHEN ETHNICITY_SUBCATEGORY = 'Other: Arab' THEN 13
WHEN ETHNICITY_SUBCATEGORY = 'Other: Other' THEN 14
WHEN ETHNICITY_SUBCATEGORY = 'White: British' THEN 15
WHEN ETHNICITY_SUBCATEGORY = 'White: Irish' THEN 16
WHEN ETHNICITY_SUBCATEGORY = 'White: Traveller' THEN 17
WHEN ETHNICITY_SUBCATEGORY = 'White: Other White' THEN 18
WHEN ETHNICITY_SUBCATEGORY = 'Unknown' THEN 19
WHEN ETHNICITY_SUBCATEGORY = 'Not Recorded' THEN 19
WHEN ETHNICITY_SUBCATEGORY = 'Not stated' THEN 19
WHEN ETHNICITY_SUBCATEGORY = 'Not Stated' THEN 19
WHEN ETHNICITY_SUBCATEGORY = 'Recorded Not Known' THEN 19
WHEN ETHNICITY_SUBCATEGORY = 'Refused' THEN 19
END AS ETHSUBCAT_ORDER
from pop