{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['smi_registry']
        )
}}
--Create a person level dataset for where the key filters are number of checks missed in last 12 months and vulnerabilities such as smoking, substance misuse, homelessness, deprivation and LTC count
--number of incomplete or declined health checks in last 12 months for those with and active SMI diagnosis. Exclude people who are on lithium alone. 
with healthcheck_sum as (
SELECT DISTINCT
  PERSON_ID
  ,MAX(CASE WHEN CHECK_TYPE = 'Smoking' AND check_status in ('Not Met','Declined') THEN 'Yes' ELSE 'No' END) as SMOK_MISS
  ,MAX(CASE WHEN CHECK_TYPE = 'Blood Pressure' AND check_status in ('Not Met','Declined') THEN 'Yes' ELSE 'No' END) as BP_MISS
  ,MAX(CASE WHEN CHECK_TYPE = 'Alcohol' AND check_status in ('Not Met','Declined') THEN 'Yes' ELSE 'No' END) as ALC_MISS
  ,MAX(CASE WHEN CHECK_TYPE = 'Cholesterol' AND check_status in ('Not Met','Declined') THEN 'Yes' ELSE 'No' END) as CHOL_MISS
  ,MAX(CASE WHEN CHECK_TYPE = 'BMI' AND check_status in ('Not Met','Declined') THEN 'Yes' ELSE 'No' END) as BMI_MISS
  ,MAX(CASE WHEN CHECK_TYPE = 'HBA1C' AND check_status in ('Not Met','Declined') THEN 'Yes' ELSE 'No' END) as HBA1C_MISS
  ,LISTAGG(CASE 
  WHEN CHECK_TYPE = 'Smoking' THEN 'Smok'
  WHEN CHECK_TYPE = 'Blood Pressure' THEN 'BP'
  WHEN CHECK_TYPE = 'Alcohol' THEN 'Alc'
  WHEN CHECK_TYPE = 'BMI' THEN 'BMI'
  WHEN CHECK_TYPE = 'Cholesterol' THEN 'Chol'
  WHEN CHECK_TYPE = 'HBA1C' THEN 'HbA1c'
  --Coderabbit suggestion:If consistent output matters (e.g. for downstream comparisons or testing), consider adding WITHIN GROUP. It's good practice.
  ELSE CHECK_TYPE END ,',') WITHIN GROUP (ORDER BY CHECK_TYPE) AS INCOMP12M_LIST
   ,COUNT(DISTINCT CHECK_TYPE) AS INCOMP12M_CT
--FROM MODELLING.OLIDS_PROGRAMME.INT_SMI_SIX_HEALTH_CHECK 
FROM {{ ref('int_smi_six_health_check')  }} 
WHERE check_status in ('Not Met','Declined')
Group by ALL
)
--FIND PEOPLE WHO HAVE DECLINED A HEALTH CHECK
,declined as (
select distinct person_id
--FROM MODELLING.OLIDS_PROGRAMME.INT_SMI_SIX_HEALTH_CHECK
FROM {{ ref('int_smi_six_health_check')  }}
WHERE check_status = 'Declined' OR exception_category is not null
)
select 
p.PERSON_ID
,p.HX_FLAKE
,p.PRACTICE_NAME
,p.PRACTICE_CODE
,p.PRIMARY_CARE_NETWORK
,p.PRACTICE_BOROUGH
,COALESCE(hc.INCOMP12M_CT,0) AS INCOMP12M_CT
,COALESCE(hc.INCOMP12M_LIST, 'All checks met') AS INCOMP12M_LIST
,COALESCE(hc.SMOK_MISS,'No') AS SMOK_MISS
,COALESCE(hc.ALC_MISS,'No') AS ALC_MISS
,COALESCE(hc.BP_MISS,'No') AS BP_MISS
,COALESCE(hc.CHOL_MISS,'No') AS CHOL_MISS
,COALESCE(hc.BMI_MISS,'No') AS BMI_MISS
,COALESCE(hc.HBA1C_MISS,'No') AS HBA1C_MISS
,CASE WHEN dc.PERSON_ID IS NOT NULL THEN 'Yes' ELSE 'No' END AS HAS_DECLINED
,AGE
,AGE_BAND_5Y
,GENDER
,ETHNICITY_CATEGORY
,ETHCAT_ORDER
,ETHNICITY_SUBCATEGORY
,ETHSUBCAT_ORDER
,ETHNICITY_GRANULAR
,MAIN_LANGUAGE
,INTERPRETER_TYPE
,IFF(IS_HOMELESS, 'Yes', 'No') AS IS_HOMELESS
,IMD_QUINTILE
,IMDQUINTILE_ORDER
,IFF(INTERPRETER_NEEDED = TRUE, 'Yes', 'No') AS INTERPRETER_NEEDED
,IFF(l.smoking_status = 'Current Smoker', 'Yes', 'No') AS IS_SMOKER
,l.ILLICIT_DRUG_PATTERN
,CASE 
WHEN l.drug_misuse_flag = 'Yes' THEN 'Hx harmful use'
WHEN l.drug_misuse_flag = 'No' THEN 'Non-user'
ELSE 'Unknown' END as DRUG_USE
,l.illicit_drug_date AS LATEST_DRUG_DATE
,l.ALCOHOL_RISK_CATEGORY AS LATEST_ALCOHOL_EVER
,CASE
WHEN l.high_alcohol_use_flag = 'Yes' THEN 'Higher Risk'
WHEN l.high_alcohol_use_flag = 'No' THEN 'Low risk/Non drinker'
WHEN l.ALCOHOL_RISK_CATEGORY = 'Unclear' or l.ALCOHOL_RISK_CATEGORY is NULL THEN 'Unclear/Unknown' 
WHEN l.ALCOHOL_RISK_CATEGORY = 'Alcohol Status Declined' THEN 'Status Declined' END AS ALCOHOL_USE
,l.alc_stat_date AS LATEST_ALCOHOL_DATE
,NVL(ltc.LTC_COUNT,0) AS LTC_COUNT
,CASE WHEN ltc.LTC_COUNT >= 2 THEN 'Yes' ELSE 'No' END AS LTC_2PLUS
,ltc.LTC_SUMMARY
,IS_ON_LITHIUM
--FROM MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p
FROM {{ ref('int_smi_population_base')  }} p
LEFT JOIN healthcheck_sum hc using (person_id)
LEFT JOIN declined dc using (person_id)
--LEFT JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_LIFESTYLE l using (person_id)
LEFT JOIN {{ ref('int_smi_lifestyle')  }} l using (person_id)
--LEFT JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_LTC_PROFILE ltc using (person_id)
LEFT JOIN {{ ref('int_smi_ltc_profile')  }} ltc using (person_id)
WHERE p.HAS_ACTIVE_SMI_DIAGNOSIS = TRUE
-- include all people whether they have had a health check or not, as long as they have an active SMI diagnosis. Those with no health checks will have 0 incomplete checks and 'All checks met' in the list field. 

