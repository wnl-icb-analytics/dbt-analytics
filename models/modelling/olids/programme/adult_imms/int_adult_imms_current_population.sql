{{
    config(
        materialized='table',
        tags=['adult_imms'],
        cluster_by=['person_id'])
}}
--define PPV clinical risk groups using COVID. PLUS Cochlear Implant and CSF leak specified in UKHSA rules.
WITH PPV_clinical_risk_groups AS (
select distinct person_id
FROM (
SELECT person_id, subcohort as risk_group, reference_date
    --from REPORTING.OLIDS_PROGRAMME.FCT_FLU_ELIGIBILITY
    FROM {{ ref('fct_flu_eligibility') }}
   WHERE subcohort 
   in ('Chronic Respiratory Disease','Asplenia','Chronic Heart Disease','Chronic Kidney Disease','Diabetes','Chronic Liver Disease','Immunosuppression')
   QUALIFY ROW_NUMBER() OVER (PARTITION BY PERSON_ID, RISK_GROUP ORDER BY REFERENCE_DATE DESC) = 1

UNION ALL
SELECT person_id, 'Cochlear Implant' as risk_group, date(clinical_effective_date) as reference_date
FROM {{ ref('int_cochlear_implant_latest') }}
--FROM MODELLING.OLIDS_OBSERVATIONS.INT_COCHLEAR_IMPLANT_LATEST  

UNION ALL
SELECT person_id, 'CSF Leak' as risk_group, date(clinical_effective_date) as reference_date
FROM {{ ref('int_csf_leak_latest')}}
--FROM MODELLING.OLIDS_OBSERVATIONS.INT_CSF_LEAK_LATEST
) a
)

SELECT DISTINCT
dem.PERSON_ID
,dem.sk_patient_id
,dem.BIRTH_DATE_APPROX
,dem.AGE 
,dem.AGE_BAND_5Y
,age.AGE_DAYS_APPROX
--RSV catch up eligibility flag - turning 80 after 1st September 2024
,CASE
     WHEN dem.BIRTH_DATE_APPROX > '1943-09-01' AND dem.BIRTH_DATE_APPROX <= '1944-09-01'
    THEN TRUE ELSE FALSE 
END AS TURN_80_AFTER_SEP_2024
--RSV routine - turning 75-79 after 1st September 2024
,CASE
    WHEN dem.BIRTH_DATE_APPROX > '1944-09-01' AND dem.BIRTH_DATE_APPROX <= '1949-09-01'
    THEN TRUE ELSE FALSE 
END AS TURN_75_AFTER_SEP_2024
--Shingles programme eligibility flag - turning 65 after 1st September 2023
,CASE
    WHEN dem.BIRTH_DATE_APPROX > '1957-09-01' AND dem.BIRTH_DATE_APPROX <= '1958-09-01'
    THEN TRUE ELSE FALSE 
END AS TURN_65_AFTER_SEP_2023
--use the general care home flag as this is more complete than the COVID group.
,CASE WHEN ch.PERSON_ID IS NOT NULL THEN TRUE ELSE FALSE END AS IS_CARE_HOME_RESIDENT
--general immunosuppression flag for Shingles programme eligibility
,CASE WHEN imm.PERSON_ID IS NOT NULL THEN TRUE ELSE FALSE END AS IS_IMMUNOSUPPRESSED
--PPV clinical risk group flag which includes immunosuppression but also other risk groups eligible for PPV
,CASE WHEN ppv.PERSON_ID IS NOT NULL THEN TRUE ELSE FALSE END AS IN_PPV_CLINICAL_RISK_GROUP
,CASE WHEN preg.PERSON_ID IS NOT NULL THEN TRUE ELSE FALSE END AS IS_PREGNANT
,dem.GENDER
,CASE
WHEN dem.ETHNICITY_CATEGORY = 'Not Recorded' THEN 'Unknown'
ELSE dem.ETHNICITY_CATEGORY END AS ETHNICITY_CATEGORY
,CASE 
WHEN dem.ETHNICITY_CATEGORY = 'Asian' THEN 1
WHEN dem.ETHNICITY_CATEGORY = 'Black' THEN 2
WHEN dem.ETHNICITY_CATEGORY = 'Mixed' THEN 3
WHEN dem.ETHNICITY_CATEGORY = 'Other' THEN 4
WHEN dem.ETHNICITY_CATEGORY = 'White' THEN 5
WHEN dem.ETHNICITY_CATEGORY = 'Unknown' THEN 6
WHEN dem.ETHNICITY_CATEGORY = 'Not Recorded' THEN 6
END AS ETHCAT_ORDER 
,CASE
WHEN dem.ETHNICITY_SUBCATEGORY in ('Not Recorded','Not stated','Not Stated','Recorded Not Known','Refused') THEN 'Unknown'
ELSE dem.ETHNICITY_SUBCATEGORY END AS ETHNICITY_SUBCATEGORY
,CASE 
WHEN dem.ETHNICITY_SUBCATEGORY = 'Asian: Bangladeshi' THEN 1
WHEN dem.ETHNICITY_SUBCATEGORY = 'Asian: Chinese' THEN 2
WHEN dem.ETHNICITY_SUBCATEGORY = 'Asian: Indian' THEN 3
WHEN dem.ETHNICITY_SUBCATEGORY = 'Asian: Pakistani' THEN 4
WHEN dem.ETHNICITY_SUBCATEGORY = 'Asian: Other Asian' THEN 5
WHEN dem.ETHNICITY_SUBCATEGORY = 'Black: African' THEN 6
WHEN dem.ETHNICITY_SUBCATEGORY = 'Black: Caribbean' THEN 7
WHEN dem.ETHNICITY_SUBCATEGORY = 'Black: Other Black' THEN 8
WHEN dem.ETHNICITY_SUBCATEGORY = 'Mixed: White and Asian' THEN 9
WHEN dem.ETHNICITY_SUBCATEGORY = 'Mixed: White and Black African' THEN 10
WHEN dem.ETHNICITY_SUBCATEGORY = 'Mixed: White and Black Caribbean' THEN 11
WHEN dem.ETHNICITY_SUBCATEGORY = 'Mixed: Other Mixed' THEN 12
WHEN dem.ETHNICITY_SUBCATEGORY = 'Other: Arab' THEN 13
WHEN dem.ETHNICITY_SUBCATEGORY = 'Other: Other' THEN 14
WHEN dem.ETHNICITY_SUBCATEGORY = 'White: British' THEN 15
WHEN dem.ETHNICITY_SUBCATEGORY = 'White: Irish' THEN 16
WHEN dem.ETHNICITY_SUBCATEGORY = 'White: Traveller' THEN 17
WHEN dem.ETHNICITY_SUBCATEGORY = 'White: Other White' THEN 18
WHEN dem.ETHNICITY_SUBCATEGORY = 'Unknown' THEN 19
WHEN dem.ETHNICITY_SUBCATEGORY = 'Not Recorded' THEN 19
WHEN dem.ETHNICITY_SUBCATEGORY = 'Not stated' THEN 19
WHEN dem.ETHNICITY_SUBCATEGORY = 'Not Stated' THEN 19
WHEN dem.ETHNICITY_SUBCATEGORY = 'Recorded Not Known' THEN 19
WHEN dem.ETHNICITY_SUBCATEGORY = 'Refused' THEN 19
END AS ETHSUBCAT_ORDER
,CASE 
WHEN dem.ETHNICITY_GRANULAR = 'MENA' THEN 'Middle East and North African'
WHEN dem.ETHNICITY_GRANULAR in ('Muslim', 'Sikh') THEN 'Unknown'
WHEN dem.ETHNICITY_GRANULAR in ('Recorded Not Known', 'Refused', 'Not stated', 'Not Recorded','Not Stated') THEN 'Unknown'
WHEN dem.ETHNICITY_GRANULAR = 'Black - E.African Asian' THEN 'East African Asian'
WHEN dem.ETHNICITY_GRANULAR = 'Indo-Caribbean' THEN 'Caribbean Asian'
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
,COALESCE(la.LAD25_NM,'Unknown') as RESIDENTIAL_BOROUGH
,COALESCE(dem.NEIGHBOURHOOD_RESIDENT,'Unknown') as RESIDENTIAL_NEIGHBOURHOOD
,COALESCE(la.RESIDENT_FLAG,'Unknown') as RESIDENTIAL_LOC
,dem.WARD_CODE
,dem.WARD_NAME
,dem.LSOA_CODE_21
,dem.IS_ACTIVE
,dem.is_deceased
FROM {{ ref('dim_person_demographics') }} dem
LEFT JOIN {{ ref('dim_person_age') }} age using (PERSON_ID)
LEFT JOIN {{ ref('stg_reference_lsoa21_ward25_lad25') }} la on la.LSOA21_CD = dem.LSOA_CODE_21
--LEFT JOIN REPORTING.OLIDS_PERSON_STATUS.DIM_PERSON_CARE_HOME
LEFT JOIN {{ ref('dim_person_care_home') }} ch using (PERSON_ID)
LEFT JOIN (SELECT DISTINCT PERSON_ID FROM {{ ref('int_covid_immunosuppression') }}) imm on imm.person_id = dem.person_id
--LEFT JOIN (SELECT DISTINCT PERSON_ID FROM MODELLING.OLIDS_PROGRAMME.INT_COVID_IMMUNOSUPPRESSION) imm on imm.person_id = dem.person_id
LEFT JOIN PPV_clinical_risk_groups ppv on ppv.person_id = dem.person_id
LEFT JOIN (select person_id from {{ ref('fct_person_pregnancy_status') }} where is_child_bearing_age_12_55) preg on preg.person_id = dem.person_id
--LEFT JOIN (select person_id from REPORTING.OLIDS_PERSON_STATUS.fct_person_pregnancy_status where is_child_bearing_age_12_55) preg on preg.person_id = dem.person_id
WHERE dem.is_active 
AND dem.IS_DECEASED = FALSE
--remove lower age limit to include residents needing the Shingles vaccine aged 18+ and currently pregnant women (of childbearing age 12-55). 
AND dem.age >= 18