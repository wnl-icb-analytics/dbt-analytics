{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['smi_registry']
        )
}}
-- Intermediate table holding core health checks for people on the SMI register but not for people who are on lithium alone. MH1_REG only
/* Intermediate Model to capture 6 measurements including exceptions and declined */
WITH EXCEPTIONS AS (
select 
e.person_id
,DATE(e.clinical_effective_date) as EXCEPTION_DATE
,CASE 
WHEN e.concept_display = 'Excepted from mental health quality indicators - patient unsuitable' THEN 'Patient Unsuitable' ELSE 'Informed dissent' 
END as EXCEPTION_CATEGORY
FROM {{ ref('int_smi_quality_check_exception') }} e
INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)
where e.clinical_effective_date  >= DATEADD('month', -12, CURRENT_DATE)
QUALIFY ROW_NUMBER() OVER (PARTITION BY e.person_id ORDER BY e.clinical_effective_date DESC) = 1
)


,BMI as (
select * 
FROM (
--latest BMI code in the last year
select 
b.person_id
,'BMI' as TEST_TYPE
,DATE(b.clinical_effective_date) as BMI_date
,b.bmi_category
,b.BMI_VALUE
FROM {{ ref('int_bmi_latest') }} b
INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)
where clinical_effective_date  >= DATEADD('month', -12, CURRENT_DATE)

UNION

--latest BMI declined code in the last year
select 
b.person_id
,'BMI' as TEST_TYPE
,DATE(b.clinical_effective_date) as BMI_DATE
,'BMI Declined' as bmi_category
,NULL as BMI_VALUE
FROM {{ ref('int_bmi_assessment_declined') }} b
INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)
where b.clinical_effective_date  >= DATEADD('month', -12, CURRENT_DATE)
QUALIFY ROW_NUMBER() OVER (PARTITION BY b.person_id ORDER BY b.clinical_effective_date DESC) = 1
) a
--select latest from BMI code or Declined in the last year
QUALIFY ROW_NUMBER() OVER (PARTITION BY a.person_id ORDER BY a.bmi_date DESC, CASE WHEN a.bmi_category <> 'BMI Declined' THEN 1 ELSE 0 END DESC) = 1
)
-- # TEST 2 GLUCOSE/HBA1C
,Glucose as (
select * 
FROM (
select 
b.person_id
,'HbA1c' as TEST_TYPE
,DATE(b.clinical_effective_date) as glucose_date
,b.HBA1C_CATEGORY AS GLUCOSE_CATEGORY
,b.HBA1C_DISPLAY AS GLUCOSE_DISPLAY
FROM {{ ref('int_hba1c_latest') }} b
INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)
where clinical_effective_date  >= DATEADD('month', -12, CURRENT_DATE)

UNION
select 
b.person_id
,'Blood Glucose' as TEST_TYPE
,DATE(b.clinical_effective_date) as glucose_date
,NULL AS GLUCOSE_CATEGORY
,b.BLOOD_GLUCOSE_DISPLAY AS GLUCOSE_DISPLAY
FROM {{ ref('int_blood_glucose_latest') }} b
INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)
where clinical_effective_date  >= DATEADD('month', -12, CURRENT_DATE)

UNION

select 
b.person_id
,'Blood glucose' as TEST_TYPE
,DATE(b.clinical_effective_date) as glucose_date
,'Glucose Declined' as glucose_category
,NULL AS glucose_display
FROM {{ ref('int_blood_glucose_declined') }} b
INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)
where b.clinical_effective_date  >= DATEADD('month', -12, CURRENT_DATE)
QUALIFY ROW_NUMBER() OVER (PARTITION BY b.person_id ORDER BY b.clinical_effective_date DESC) = 1
) a
QUALIFY ROW_NUMBER() OVER (PARTITION BY a.person_id ORDER BY a.glucose_date DESC, CASE WHEN a.glucose_category <> 'Glucose Declined' THEN 1 ELSE 0 END DESC) = 1
)
-- # TEST 3 CHOLESTEROL OR QRISK
,CHOL as (
select * 
FROM (
select 
c.person_id
,'Cholesterol' as TEST_TYPE
,DATE(c.clinical_effective_date) as Cholesterol_date
,c.CHOLESTEROL_CATEGORY
,c.CHOLESTEROL_VALUE
FROM {{ ref('int_cholesterol_latest') }} c
INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)
where clinical_effective_date  >= DATEADD('month', -12, CURRENT_DATE)

UNION
select 
q.person_id
,'QRISK' as TEST_TYPE
,DATE(q.clinical_effective_date) as Cholesterol_date
,q.cvd_risk_category as CHOLESTEROL_CATEGORY
,q.qrisk_score as CHOLESTEROL_VALUE
FROM {{ ref('int_qrisk_latest') }} q
--FROM MODELLING.OLIDS_OBSERVATIONS.INT_QRISK_LATEST q
INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p USING (PERSON_ID)
where clinical_effective_date  >= DATEADD('month', -12, CURRENT_DATE)

UNION
select 
c.person_id
,'Cholesterol' as TEST_TYPE
,DATE(c.clinical_effective_date) as Cholesterol_date
,'Cholesterol Declined' as CHOLESTEROL_CATEGORY
,NULL as CHOLESTEROL_VALUE
FROM {{ ref('int_cholesterol_declined') }} c
INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)
where c.clinical_effective_date  >= DATEADD('month', -12, CURRENT_DATE)
QUALIFY ROW_NUMBER() OVER (PARTITION BY c.person_id ORDER BY c.clinical_effective_date DESC) = 1
) a
QUALIFY ROW_NUMBER() OVER (PARTITION BY a.person_id ORDER BY a.Cholesterol_date DESC, CASE WHEN a.CHOLESTEROL_CATEGORY <> 'Cholesterol Declined' THEN 1 ELSE 0 END DESC) = 1
)
-- # TEST 4 BLOOD PRESSURE
,BP as (
SELECT *
FROM (
select 
bp.person_id
     ,'Blood Pressure' as TEST_TYPE
     ,DATE(bp.LATEST_BP_DATE) as BP_date
     ,bp.LATEST_SYSTOLIC_VALUE
     ,bp.LATEST_DIASTOLIC_VALUE
     ,CASE 
          WHEN bp.IS_OVERALL_BP_CONTROLLED = 'Yes' THEN 'Controlled' 
          WHEN bp.IS_OVERALL_BP_CONTROLLED = 'No' THEN 'Not Controlled' 
          ELSE NULL END AS BP_CATEGORY
FROM {{ ref('fct_person_bp_control') }} bp
INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)
where LATEST_BP_DATE  >= DATEADD('month', -12, CURRENT_DATE)
And bp.LATEST_BP_DATE <= CURRENT_DATE()

UNION

select 
     bp.person_id
     ,'Blood Pressure' as TEST_TYPE
     ,DATE(bp.clinical_effective_date) as BP_date
     ,NULL AS LATEST_SYSTOLIC_VALUE
     ,NULL AS LATEST_DIASTOLIC_VALUE
     ,'BP Declined' AS BP_CATEGORY
FROM {{ ref('int_blood_pressure_declined') }} bp
INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)
where bp.clinical_effective_date  >= DATEADD('month', -12, CURRENT_DATE)
QUALIFY ROW_NUMBER() OVER (PARTITION BY bp.person_id ORDER BY bp.clinical_effective_date DESC) = 1
) a
QUALIFY ROW_NUMBER() OVER (PARTITION BY a.person_id ORDER BY a.BP_date DESC, CASE WHEN a.BP_CATEGORY <> 'BP Declined' THEN 1 ELSE 0 END DESC) = 1
)
-- # TEST 5 SMOKING STATUS
,SMOK as (
SELECT * 
FROM (
select 
s.person_id
,'Smoking status' as TEST_TYPE
,DATE(s.LATEST_SMOKING_DATE) as smoking_date
,s.SMOKING_STATUS
FROM {{ ref('fct_person_smoking_status') }} s
INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)
where LATEST_SMOKING_DATE  >= DATEADD('month', -12, CURRENT_DATE)

UNION

select 
s.person_id
,'Smoking status' as TEST_TYPE
,DATE(s.clinical_effective_date) as smoking_date
,'Smoking Declined' AS SMOKING_STATUS
FROM {{ ref('int_smoking_assessment_declined') }} s
INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)
where s.clinical_effective_date  >= DATEADD('month', -12, CURRENT_DATE)
QUALIFY ROW_NUMBER() OVER (PARTITION BY s.person_id ORDER BY s.clinical_effective_date DESC) = 1
) a
QUALIFY ROW_NUMBER() OVER (PARTITION BY a.person_id ORDER BY a.SMOKING_date DESC, CASE WHEN a.SMOKING_STATUS <> 'Smoking Declined' THEN 1 ELSE 0 END DESC) = 1
)
--TEST # 6 ALCOHOL ASSESSMENT EITHER AUDIT C or UNITS PER WEEK
,ALC as (
SELECT * 
FROM (
select 
a.person_id
,'AUDIT' as TEST_TYPE
,DATE(a.LATEST_AUDIT_DATE) as alcohol_assessment_date
,a.AUDIT_RISK_CATEGORY as ALCOHOL_RISK_CATEGORY
,NULL as alcohol_units
,NULL as unit_display
FROM {{ ref('fct_person_alcohol_status') }} a
INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)
where DATE(a.LATEST_AUDIT_DATE)  >= DATEADD('month', -12, CURRENT_DATE)

UNION

select 
a.person_id
,'Units per week' as TEST_TYPE
,a.clinical_effective_date as alcohol_assessment_date
,a.alcohol_risk_category
,a.result_value as alcohol_units
,a.result_unit_display as unit_display
FROM {{ ref('int_alcohol_units_latest') }} a
INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)
where DATE(clinical_effective_date)  >= DATEADD('month', -12, CURRENT_DATE)

UNION

select 
s.person_id
,'AUDIT' as TEST_TYPE
,DATE(s.clinical_effective_date) as alcohol_assessment_date
,'Alcohol Declined' AS ALCOHOL_RISK_CATEGORY
,NULL as alcohol_units
,NULL as unit_display
FROM {{ ref('int_alcohol_assessment_declined') }} s
INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)
where s.clinical_effective_date  >= DATEADD('month', -12, CURRENT_DATE)
QUALIFY ROW_NUMBER() OVER (PARTITION BY s.person_id ORDER BY s.clinical_effective_date DESC) = 1
) a
QUALIFY ROW_NUMBER() OVER (PARTITION BY a.person_id ORDER BY a.alcohol_assessment_date DESC, 
CASE WHEN ALCOHOL_RISK_CATEGORY NOT IN ('Unclear','Alcohol Declined') THEN 2 
WHEN ALCOHOL_RISK_CATEGORY = 'Alcohol Declined' THEN 1 ELSE 0 END DESC) = 1
)
,HC_CHECKS AS (
    SELECT
        p.person_id,
        CASE WHEN b.person_id IS NULL THEN 'Not Met'
             WHEN b.person_id IS NOT NULL AND b.BMI_CATEGORY = 'BMI Declined' THEN 'Declined'
             ELSE 'Met' END AS BMI_CHECK_12M,
        CASE WHEN h.person_id IS NULL THEN 'Not Met'
             WHEN h.person_id IS NOT NULL AND h.GLUCOSE_CATEGORY = 'Glucose Declined' THEN 'Declined'
             ELSE 'Met' END AS HBA1C_CHECK_12M,
        CASE WHEN c.person_id IS NULL THEN 'Not Met'
             WHEN c.person_id IS NOT NULL AND c.CHOLESTEROL_CATEGORY = 'Cholesterol Declined' THEN 'Declined'
             ELSE 'Met' END AS CHOLESTEROL_CHECK_12M,
        CASE WHEN bp.person_id IS NULL THEN 'Not Met'
             WHEN bp.person_id IS NOT NULL AND bp.BP_CATEGORY = 'BP Declined' THEN 'Declined'
             ELSE 'Met' END AS BP_CHECK_12M,
        CASE WHEN s.person_id IS NULL THEN 'Not Met'
             WHEN s.person_id IS NOT NULL AND s.SMOKING_STATUS = 'Smoking Declined' THEN 'Declined'
             ELSE 'Met' END AS SMOKING_CHECK_12M,
        CASE WHEN a.person_id IS NULL THEN 'Not Met'
             WHEN a.person_id IS NOT NULL AND a.alcohol_risk_category = 'Alcohol Declined' THEN 'Declined'
             ELSE 'Met' END AS ALCOHOL_CHECK_12M
    FROM {{ ref('int_smi_population_base')  }} p
    LEFT JOIN BMI b USING (person_id)
    LEFT JOIN Glucose h USING (person_id)
    LEFT JOIN CHOL c USING (person_id)
    LEFT JOIN BP bp USING (person_id)
    LEFT JOIN SMOK s USING (person_id)
    LEFT JOIN ALC a USING (person_id)
)
,COMBINED as (
SELECT person_id, 'BMI' AS Check_Type, b.test_type, BMI_CHECK_12M AS Check_Status,
       b.BMI_DATE AS Check_Date,  b.bmi_category AS Result_Category, CAST(b.BMI_VALUE AS VARCHAR) AS Value
FROM HC_CHECKS hc
LEFT JOIN BMI b USING (person_id)

UNION ALL

SELECT person_id, 'HBA1C' AS Check_Type, h.test_type, HBA1C_CHECK_12M AS Check_Status,
       h.glucose_date AS Check_Date, h.glucose_category AS Result_Category, h.glucose_display AS Value
FROM HC_CHECKS hc
LEFT JOIN Glucose h USING (person_id)

UNION ALL

SELECT person_id, 'Cholesterol' AS Check_Type, c.test_type, CHOLESTEROL_CHECK_12M AS Check_Status,
       c.Cholesterol_date AS Check_Date, c.CHOLESTEROL_CATEGORY AS Result_Category, CAST(c.CHOLESTEROL_VALUE AS VARCHAR) AS Value
FROM HC_CHECKS hc
LEFT JOIN CHOL c USING (person_id)

UNION ALL

SELECT person_id, 'Blood Pressure' AS Check_Type, bp.test_type, BP_CHECK_12M AS Check_Status,
       bp.BP_DATE AS Check_Date, 
       bp.bp_category AS Result_Category,
       CAST(bp.LATEST_SYSTOLIC_VALUE AS VARCHAR) || '/' || CAST(bp.LATEST_DIASTOLIC_VALUE AS VARCHAR) AS Value
       FROM HC_CHECKS hc
LEFT JOIN BP bp USING (person_id)

UNION ALL

SELECT person_id, 'Smoking' AS Check_Type, s.test_type, SMOKING_CHECK_12M AS Check_Status,
       s.SMOKING_DATE AS Check_Date, s.SMOKING_STATUS AS Result_Category, NULL AS Value
FROM HC_CHECKS hc
LEFT JOIN SMOK s USING (person_id)

UNION ALL

SELECT person_id, 'Alcohol' AS Check_Type, a.test_type, ALCOHOL_CHECK_12M AS Check_Status,
       a.alcohol_assessment_date AS Check_Date, a.alcohol_risk_category AS Result_Category,
       CAST(a.alcohol_units AS VARCHAR) || ' ' || CAST(a.unit_display AS VARCHAR) AS Value
       FROM HC_CHECKS hc
LEFT JOIN ALC a USING (person_id)
)
,HC_CHECK AS (
select
p.PERSON_ID
,p.HX_FLAKE
,p.PRACTICE_BOROUGH 
,p.PRACTICE_NEIGHBOURHOOD
,p.PRIMARY_CARE_NETWORK
,p.PRACTICE_NAME 
,p.PRACTICE_CODE
,p.RESIDENTIAL_BOROUGH
,p.RESIDENTIAL_NEIGHBOURHOOD
,p.RESIDENTIAL_LOC
,p.WARD_CODE
,p.WARD_NAME
,p.AGE
,p.AGE_BAND_5Y
,p.AGE_BAND_NHS
,p.AGE_NHS_ORDER
,p.GENDER
,p.ETHNICITY_CATEGORY
,p.ETHCAT_ORDER
,p.ETHNICITY_SUBCATEGORY
,p.ETHSUBCAT_ORDER
,p.IMD_QUINTILE
,p.IMDQUINTILE_ORDER
,p.MAIN_LANGUAGE
,e.EXCEPTION_DATE
,e.EXCEPTION_CATEGORY
,hc.TEST_TYPE
,hc.CHECK_DATE
,hc.CHECK_TYPE
,hc.CHECK_STATUS
,hc.RESULT_CATEGORY
,hc.VALUE
FROM {{ ref('int_smi_population_base')  }} p
LEFT JOIN EXCEPTIONS e USING (PERSON_ID)
LEFT JOIN COMBINED hc USING (PERSON_ID)
--include only people with active SMI diagnosis as eligible for a health check- they might also be on lithium
WHERE p.HAS_ACTIVE_SMI_DIAGNOSIS = TRUE

)
,MET_COUNT as (
SELECT DISTINCT
  PERSON_ID
 ,COUNT(DISTINCT CHECK_TYPE) AS MET_CT
   FROM HC_CHECK 
   WHERE CHECK_STATUS = 'Met'
GROUP by ALL
)
--FINAL OUTPUT
SELECT hc.*,
NVL(mc.MET_CT,0) AS MET_CT
FROM HC_CHECK hc
left JOIN MET_COUNT mc using (PERSON_ID)