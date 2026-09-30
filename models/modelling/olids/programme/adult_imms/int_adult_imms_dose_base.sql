{{
    config(
        materialized='table',
        tags=['adult_imms'])
}}
--define PPV clinical risk groups using COVID. PLUS Cochlear Implant and CSF leak specified in UKHSA rules.
WITH PPV_clinical_risk_groups AS (
select distinct person_id
FROM (
SELECT person_id, risk_group, reference_date
    --from REPORTING.OLIDS_PROGRAMME.FCT_COVID_ELIGIBILITY
    FROM {{ ref('fct_covid_eligibility') }}
   WHERE risk_group 
   in ('Chronic Respiratory Disease','Asplenia/Spleen Dysfunction','Chronic Heart Disease','Chronic Kidney Disease','Diabetes','Chronic Liver Disease','Immunosuppression')
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
--September 2026 add new group for RSV clinical risk groups which includes immunosuppression and chronic lung disease.
,RSV_clinical_risk_groups AS (
select distinct person_id
FROM (
SELECT person_id, subcohort as risk_group, reference_date
    --from REPORTING.OLIDS_PROGRAMME.FCT_FLU_ELIGIBILITY
    FROM {{ ref('fct_flu_eligibility') }}
   WHERE subcohort 
   in ('Chronic Respiratory Disease','Immunosuppression')
   QUALIFY ROW_NUMBER() OVER (PARTITION BY PERSON_ID, RISK_GROUP ORDER BY REFERENCE_DATE DESC) = 1
   ) a
)
--Using the person_demographics for anyone over the age of 60 who has been ever been registered with an NCL practice (active or inactive) n~200,000
--Creating a base table for vaccinations for this population to be joined against in further analysis n~2.5 million rows 
SELECT DISTINCT
p.PERSON_ID
--adding age to cross checks against FDP figures.
,CASE WHEN p.AGE >= 65 THEN TRUE ELSE FALSE END AS AGE_65_PLUS
,CASE WHEN p.AGE >= 75 THEN TRUE ELSE FALSE END AS AGE_75_PLUS
,CASE
    WHEN p.BIRTH_DATE_APPROX > '1957-09-01' AND p.BIRTH_DATE_APPROX <= '1958-09-01'
    THEN TRUE ELSE FALSE 
END AS TURN_65_AFTER_SEP_2023
,p.AGE_BAND_5Y
,p.practice_code
,CASE WHEN ch.PERSON_ID IS NOT NULL THEN TRUE ELSE FALSE END AS IS_CARE_HOME_RESIDENT
--general immunosuppression flag for Shingles programme eligibility
,CASE WHEN imm.PERSON_ID IS NOT NULL THEN TRUE ELSE FALSE END AS IS_IMMUNOSUPPRESSED
,CASE WHEN ppv.PERSON_ID IS NOT NULL THEN TRUE ELSE FALSE END AS IN_PPV_CLINICAL_RISK_GROUP
,CASE WHEN rsv.PERSON_ID IS NOT NULL THEN TRUE ELSE FALSE END AS IN_RSV_CLINICAL_RISK_GROUP
,CASE WHEN preg.PERSON_ID IS NOT NULL THEN TRUE ELSE FALSE END AS IS_PREGNANT
,CASE
WHEN p.ETHNICITY_CATEGORY = 'Not Recorded' THEN 'Unknown'
ELSE p.ETHNICITY_CATEGORY END AS ETHNICITY_CATEGORY
,CASE 
WHEN p.ETHNICITY_CATEGORY = 'Asian' THEN 1
WHEN p.ETHNICITY_CATEGORY = 'Black' THEN 2
WHEN p.ETHNICITY_CATEGORY = 'Mixed' THEN 3
WHEN p.ETHNICITY_CATEGORY = 'Other' THEN 4
WHEN p.ETHNICITY_CATEGORY = 'White' THEN 5
WHEN p.ETHNICITY_CATEGORY = 'Unknown' THEN 6
WHEN p.ETHNICITY_CATEGORY = 'Not Recorded' THEN 6
END AS ETHCAT_ORDER 
,COALESCE(p.IMD_QUINTILE_25, 'Unknown') AS IMD_QUINTILE
,CASE 
WHEN p.IMD_QUINTILE_25 = 'Most Deprived' THEN 1
WHEN p.IMD_QUINTILE_25 = 'Second Most Deprived' THEN 2
WHEN p.IMD_QUINTILE_25 = 'Third Most Deprived' THEN 3
WHEN p.IMD_QUINTILE_25 = 'Second Least Deprived' THEN 4
WHEN p.IMD_QUINTILE_25 = 'Least Deprived' THEN 5
ELSE 6 END AS IMDQUINTILE_ORDER
,v.VACCINE_ID
,v.VACCINE_ORDER
,v.EVENT_DATE
,v.EVENT_TYPE
,IFF(MONTH(v.event_date) >= 4, YEAR(v.event_date), YEAR(v.event_date) - 1)
  || '/'
  || LPAD(RIGHT(TO_VARCHAR(IFF(MONTH(v.event_date) >= 4, YEAR(v.event_date) + 1, YEAR(v.event_date))), 2), 2, '0')
    AS FISCAL_YEAR
FROM {{ ref('dim_person_demographics') }} p
--FROM REPORTING.OLIDS_PERSON_DEMOGRAPHICS.DIM_PERSON_DEMOGRAPHICS p
LEFT JOIN {{ ref('int_adult_imms_vaccination_events_historical') }} v on v.person_id = p.person_id
LEFT JOIN {{ ref('dim_person_care_home') }} ch on ch.person_id = p.person_id
LEFT JOIN (SELECT DISTINCT PERSON_ID FROM {{ ref('int_covid_immunosuppression') }}) imm on imm.person_id = p.person_id
--LEFT JOIN MODELLING.OLIDS_PROGRAMME.INT_ADULT_IMMS_VACCINATION_EVENTS_HISTORICAL v using (PERSON_ID)
LEFT JOIN PPV_clinical_risk_groups ppv on ppv.person_id = p.person_id
LEFT JOIN RSV_clinical_risk_groups rsv on rsv.person_id = p.person_id
LEFT JOIN (select person_id from {{ ref('fct_person_pregnancy_status') }} where is_child_bearing_age_12_55) preg on preg.person_id = p.person_id
--restrict by AGE to 65 or over
WHERE p.age >= 18