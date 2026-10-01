{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['smi_registry']
        )
}}
--COMPILE LIFESTYLE PERSON LEVEL DATA
--illicit drug use LATEST EVER
WITH illicit as (
select 
i.person_id
,DATE(i.clinical_effective_date) as illicit_drug_date
,CASE WHEN i.illicit_drug_assessed_last_12m THEN 'Yes' ELSE 'No' END AS illicit_drug_last_12m
,i.illicit_drug_pattern
,i.illicit_drug_class
,IFF(i.ILLICIT_DRUG_PATTERN = 'Does not misuse drugs', 'No', 'Yes') AS drug_misuse_flag
--FROM MODELLING.OLIDS_OBSERVATIONS.int_smi_illicit_drug_latest i
FROM {{ ref('int_illicit_drug_use_latest') }} i
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p using (person_id)
INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)
)
--Subs misuse interventions lATEST EVER
,SUBS_MISUSE_INT as (
select 
sm.person_id
,DATE(sm.clinical_effective_date) as sm_int_date
,sm.CONCEPT_DISPLAY as sm_int_type
,IFF(sm.clinical_effective_date >= DATEADD('month', -12, CURRENT_DATE), 'Yes', 'No') AS sm_int_last_12m
,sm.subs_misuse_services
--FROM MODELLING.OLIDS_OBSERVATIONS.int_smi_longlives_subs_misuse_intervention_latest sm 
FROM {{ ref('int_subs_misuse_intervention_latest') }} sm
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p using (PERSON_ID)
INNER JOIN {{ ref('int_smi_population_base')  }} p using (PERSON_ID)
)
-- SMOKING STATUS LATEST EVER
,SMOK_EVER as (
SELECT * 
FROM (
select 
s.person_id
,DATE(s.LATEST_SMOKING_DATE) as smoking_date
,s.smoking_status
,IFF(s.LATEST_SMOKING_DATE >= DATEADD('month', -12, CURRENT_DATE), 'Yes', 'No') AS smok_last_12m
,CASE
WHEN s.SMOKING_STATUS = 'Current Smoker' THEN 'Yes' 
WHEN s.SMOKING_STATUS in ('Ex-Smoker','Never Smoked') THEN 'No'
END AS smoker_flag
--FROM REPORTING.OLIDS_PERSON_STATUS.FCT_PERSON_SMOKING_STATUS s
FROM {{ ref('fct_person_smoking_status') }} s
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p USING (PERSON_ID)
INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)

UNION

select 
s.person_id
,DATE(s.clinical_effective_date) as smoking_date
,'Smoking Status Declined' AS smoking_status
,IFF(s.clinical_effective_date >= DATEADD('month', -12, CURRENT_DATE), 'Yes', 'No') AS smok_last_12m
,NULL as smoker_flag
--FROM MODELLING.OLIDS_OBSERVATIONS.INT_SMI_SMOKING_DECLINED s
FROM {{ ref('int_smoking_assessment_declined') }} s
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p USING (PERSON_ID)
INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)
QUALIFY ROW_NUMBER() OVER (PARTITION BY s.person_id ORDER BY s.clinical_effective_date DESC) = 1
) a
QUALIFY ROW_NUMBER() OVER (PARTITION BY a.person_id ORDER BY a.SMOKING_date DESC, CASE WHEN a.SMOKING_STATUS <> 'Smoking Status Declined' THEN 1 ELSE 0 END DESC) = 1
)
--Smoking interventions LATEST EVER
,SMOK_INT as (
select 
s.person_id
,DATE(s.clinical_effective_date) as smok_int_date
,s.CONCEPT_DISPLAY as smok_int_type
,IFF(s.clinical_effective_date >= DATEADD('month', -12, CURRENT_DATE), 'Yes', 'No') AS smok_int_last_12m
,s.smoking_cessation_services
--FROM MODELLING.OLIDS_OBSERVATIONS.INT_SMI_LONGLIVES_SMOKING_INTERVENTION_latest s 
FROM {{ ref('int_smoking_intervention_latest') }} s 
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p using (PERSON_ID)
INNER JOIN {{ ref('int_smi_population_base')  }} p using (PERSON_ID)
)
--ALCOHOL ASSESSMENT EITHER AUDIT, UNITS PER WEEK, HISTORY OF DISORDERS OR DECLINED LATEST EVER BASED on latest assessment date
,ALC_EVER as (
SELECT b.*
,IFF(b.alcohol_assessment_date >= DATEADD('month', -12, CURRENT_DATE), 'Yes', 'No') AS alcohol_last_12m
,CASE
WHEN alcohol_risk_category in ('Possible Dependence','Increasing Risk','Higher Risk') THEN 'Yes'
WHEN alcohol_risk_category in ('Occasional Drinker','Lower Risk','Low Risk', 'Non-Drinker', 'Ex-Drinker') THEN 'No'
END AS high_alcohol_use_flag
FROM (
SELECT * 
FROM (
--ALCOHOL MISUSE DISORDERS
SELECT
d.person_id
,'Disorders' as source
,DATE(clinical_effective_date) as alcohol_assessment_date
,'Higher Risk' as alcohol_risk_category
,NULL as alcohol_units
,NULL as unit_display
FROM {{ ref('int_alcohol_misuse_disorders') }} d
--FROM MODELLING.OLIDS_OBSERVATIONS.INT_ALCOHOL_MISUSE_DISORDERS d
INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p USING (PERSON_ID)
QUALIFY ROW_NUMBER() OVER (PARTITION BY d.person_id ORDER BY d.clinical_effective_date DESC) = 1

UNION 
--AUDIT DATA
select 
au.person_id
,'AUDIT' as source
,DATE(au.clinical_effective_date) as alcohol_assessment_date
,au.RISK_CATEGORY as alcohol_risk_category
,NULL as alcohol_units
,NULL as unit_display
--FROM MODELLING.OLIDS_OBSERVATIONS.INT_ALCOHOL_AUDIT_SCORES au
FROM {{ ref('int_alcohol_audit_scores') }} au
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p USING (PERSON_ID)
INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)
QUALIFY ROW_NUMBER() OVER (PARTITION BY au.person_id ORDER BY au.clinical_effective_date DESC) = 1

UNION
--ALCOHOL UNITS
select 
a.person_id
,'UnitsWeek' as source
,a.clinical_effective_date as alcohol_assessment_date
,a.alcohol_risk_category
,a.result_value as alcohol_units
,a.result_unit_display as unit_display
--FROM MODELLING.OLIDS_OBSERVATIONS.int_smi_alcohol_latest a
FROM {{ ref('int_alcohol_units_latest') }} a
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p USING (PERSON_ID)
INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)

UNION
--ALCOHOl ASSESSMENT DECLINED
select 
s.person_id
,'Declined' as source
,DATE(s.clinical_effective_date) as alcohol_assessment_date
,'Alcohol Status Declined' AS alcohol_risk_category
,NULL as alcohol_units
,NULL as unit_display
--FROM MODELLING.OLIDS_OBSERVATIONS.INT_SMI_ALCOHOL_DECLINED s
FROM {{ ref('int_alcohol_assessment_declined') }} s
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p USING (PERSON_ID)
INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)
QUALIFY ROW_NUMBER() OVER (PARTITION BY s.person_id ORDER BY s.clinical_effective_date DESC) = 1
) a
QUALIFY ROW_NUMBER() OVER (PARTITION BY a.person_id ORDER BY a.alcohol_assessment_date DESC, 
CASE WHEN ALCOHOL_RISK_CATEGORY NOT IN ('Unclear','Alcohol Status Declined') THEN 2 
WHEN ALCOHOL_RISK_CATEGORY = 'Alcohol Status Declined' THEN 1 ELSE 0 END DESC) = 1
) b
)
--ALCOHOL INTERVENTION LATEST EVER
,ALCOHOL_INT as (
select 
ai.person_id
,DATE(ai.clinical_effective_date) as alc_int_date
,ai.CONCEPT_DISPLAY as alc_int_type
,IFF(ai.clinical_effective_date >= DATEADD('month', -12, CURRENT_DATE), 'Yes', 'No') AS alc_int_last_12m
,ai.alcohol_advice_services
--FROM MODELLING.OLIDS_OBSERVATIONS.int_smi_longlives_alcohol_intervention_latest ai 
FROM {{ ref('int_alcohol_intervention_latest') }} ai
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p using (PERSON_ID)
INNER JOIN {{ ref('int_smi_population_base')  }} p using (PERSON_ID)
)
--BMI latest EVER
,BMI_EVER as (
select a.* 
,IFF(a.BMI_DATE >= DATEADD('month', -12, CURRENT_DATE), 'Yes', 'No') AS bmi_last_12m
FROM (
--latest BMI code EVER
select 
b.person_id
,DATE(b.clinical_effective_date) as BMI_date
,b.bmi_category
,b.BMI_VALUE
,b.BMI_RISK_SORT_KEY
--FROM MODELLING.OLIDS_OBSERVATIONS.INT_BMI_LATEST b
FROM {{ ref('int_bmi_latest') }} b
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p USING (PERSON_ID)
INNER JOIN {{ ref('int_smi_population_base')  }} p using (PERSON_ID)

UNION

--latest BMI declined EVER
select 
b.person_id
,DATE(b.clinical_effective_date) as BMI_DATE
,'BMI Declined' as bmi_category
,NULL as BMI_VALUE
,7 as BMI_RISK_SORT_KEY
--FROM DEV__MODELLING.OLIDS_OBSERVATIONS.INT_SMI_BMI_DECLINED b
FROM {{ ref('int_bmi_assessment_declined') }} b
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p USING (PERSON_ID)
INNER JOIN {{ ref('int_smi_population_base')  }} p using (PERSON_ID)
QUALIFY ROW_NUMBER() OVER (PARTITION BY b.person_id ORDER BY b.clinical_effective_date DESC) = 1
) a
--select latest from BMI code or Declined LATEST EVER
QUALIFY ROW_NUMBER() OVER (PARTITION BY a.person_id ORDER BY a.bmi_date DESC, CASE WHEN a.bmi_category <> 'BMI Declined' THEN 1 ELSE 0 END DESC) = 1
)
--NUTRITION REVIEW LATEST EVER
,NUT_REV as (
select 
n.person_id
,DATE(n.clinical_effective_date) as nutr_rev_date
,n.CONCEPT_DISPLAY as nutr_rev_outcome
,IFF(n.clinical_effective_date >= DATEADD('month', -12, CURRENT_DATE), 'Yes', 'No') AS nutr_rev_last_12m
,n.poor_diet_flag
--FROM MODELLING.OLIDS_OBSERVATIONS.int_smi_longlives_nutrition_review_latest n 
FROM {{ ref('int_nutrition_review_latest') }} n
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p using (PERSON_ID)
INNER JOIN {{ ref('int_smi_population_base')  }} p using (PERSON_ID)
)
--WEIGHT MGMT INTERVENTION LATEST EVER
,WT_MGMT as (
select 
w.person_id
,DATE(w.clinical_effective_date) as wt_mgmt_date
,w.CONCEPT_DISPLAY as wt_mgmt_type
,IFF(w.referral_diet_advice = 'Yes' AND w.clinical_effective_date >= DATEADD('month', -12, CURRENT_DATE), 'Yes', 'No') AS wt_mgmt_int_last_12m
,w.referral_diet_advice
,IFF(w.referral_exercise_advice = 'Yes' AND w.clinical_effective_date >= DATEADD('month', -12, CURRENT_DATE), 'Yes', 'No') AS exer_int_last_12m
,w.referral_exercise_advice
--FROM MODELLING.OLIDS_OBSERVATIONS.int_smi_longlives_weight_mgmt_latest w
FROM {{ ref('int_weight_mgmt_intervention_latest') }} w
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p using (PERSON_ID)
INNER JOIN {{ ref('int_smi_population_base')  }} p using (PERSON_ID)
)
--DENTAL INSPECTION LATEST EVER
,DENTAL_INSPECTION as (
select 
d.person_id
,DATE(d.clinical_effective_date) as dental_date
,d.CONCEPT_DISPLAY as dental_type
,IFF(d.clinical_effective_date >= DATEADD('month', -12, CURRENT_DATE), 'Yes', 'No') AS dental_last_12m
--FROM MODELLING.OLIDS_OBSERVATIONS.int_smi_longlives_dental_inspection_latest d
FROM {{ ref('int_dental_inspection_latest') }} d
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p using (PERSON_ID)
INNER JOIN {{ ref('int_smi_population_base')  }} p using (PERSON_ID)
)
--EXERCISE PATTERN LATEST EVER
,EXERCISE as (
select 
e.person_id
,DATE(e.clinical_effective_date) as EX_STAT_DATE
,e.CONCEPT_DISPLAY as EXERCISE_STATUS
,IFF(e.clinical_effective_date >= DATEADD('month', -12, CURRENT_DATE), 'Yes', 'No') AS exercise_last_12m
,e.low_exercise_flag
--FROM MODELLING.OLIDS_OBSERVATIONS.int_smi_longlives_exercise_assessment_latest e
FROM {{ ref('int_exercise_assessment_latest') }} e
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p using (PERSON_ID)
INNER JOIN {{ ref('int_smi_population_base')  }} p using (PERSON_ID)
)
--Population demographics and lifestyle factors
SELECT
p.PERSON_ID
,p.HX_FLAKE
,p.AGE
,p.GENDER
--latest illicit drug use and subs misuse service referrals
,NVL(i.illicit_drug_last_12m, 'No') AS illicit_drug_last_12m
,i.ILLICIT_DRUG_DATE
,i.ILLICIT_DRUG_PATTERN
,i.ILLICIT_DRUG_CLASS
,NVL(i.drug_misuse_flag, 'Unknown') AS drug_misuse_flag
,NVL(sm.sm_int_last_12m, 'No') AS sm_int_last_12m
,sm.SM_INT_DATE
,sm.SM_INT_TYPE
,sm.subs_misuse_services
--smoking status and interventions
,NVL(ss.smok_last_12m, 'No') AS smok_last_12m
,ss.SMOKING_DATE as SMOK_STATUS_DATE
,ss.SMOKING_STATUS
,ss.smoker_flag
,NVL(si.smok_int_last_12m, 'No') AS smok_int_last_12m
,si.SMOK_INT_DATE
,si.SMOK_INT_TYPE
,si.smoking_cessation_services
--alcohol use ever and alcohol interventions
,NVL(a.alcohol_last_12m, 'No') AS alcohol_last_12m
,a.alcohol_assessment_date as ALC_STAT_DATE
,a.source as ALC_SOURCE
,a.ALCOHOL_RISK_CATEGORY
,a.ALCOHOL_UNITS
,a.UNIT_DISPLAY
,a.high_alcohol_use_flag
,NVL(ai.alc_int_last_12m, 'No') AS alc_int_last_12m
,ai.ALC_INT_DATE
,ai.ALC_INT_TYPE
,ai.alcohol_advice_services
--weight management
,NVL(b.bmi_last_12m, 'No') AS bmi_last_12m
,b.BMI_DATE
,b.BMI_CATEGORY
,NVL(n.nutr_rev_last_12m, 'No') AS nutr_rev_last_12m
,n.NUTR_REV_DATE
,n.NUTR_REV_OUTCOME
,NVL(n.POOR_DIET_FLAG, 'Unknown') AS poor_diet_flag
,NVL(w.REFERRAL_DIET_ADVICE, 'No') AS referral_diet_advice
,NVL(w.wt_mgmt_int_last_12m, 'No') AS wt_mgmt_int_last_12m
,w.WT_MGMT_DATE
,w.WT_MGMT_TYPE
,NVL(d.dental_last_12m, 'No') AS dental_last_12m
,d.DENTAL_DATE
,d.DENTAL_TYPE
,NVL(e.exercise_last_12m, 'No') AS exercise_last_12m
,e.EX_STAT_DATE
,e.EXERCISE_STATUS
,NVL(e.LOW_EXERCISE_FLAG, 'Unknown') AS low_exercise_flag
,NVL(w.exer_int_last_12m, 'No') AS exer_int_last_12m
,NVL(w.REFERRAL_EXERCISE_ADVICE, 'No') AS referral_exercise_advice
--FROM MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p
FROM {{ ref('int_smi_population_base')  }} p
LEFT JOIN ILLICIT i using (person_id)
LEFT JOIN SUBS_MISUSE_INT sm using (person_id)
LEFT JOIN SMOK_EVER ss using (person_id)
LEFT JOIN SMOK_INT si using (person_id)
LEFT JOIN ALC_EVER a using (person_id)
LEFT JOIN ALCOHOL_INT ai using (person_id)
LEFT JOIN BMI_EVER b using (person_id)
LEFT JOIN NUT_REV n using (person_id)
LEFT JOIN WT_MGMT w using (person_id)
LEFT JOIN DENTAL_INSPECTION d using (person_id)
LEFT JOIN EXERCISE e using (person_id)
WHERE p.HAS_ACTIVE_SMI_DIAGNOSIS
