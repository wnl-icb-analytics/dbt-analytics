{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['smi_registry']
        )
}}
--ALL CHECKS PER PERSON PER MONTH AS INT_SMI_SIX_CHECKS_HISTORICAL
--TEST 1 BMI
--latest BMI code that falls within the year prior to the month end date categorise as MET or NOT MET and SMI is not resolved for that MONTH.
WITH BMI AS (
select 
b.person_id
,p.analysis_month
,DATE(b.clinical_effective_date) as BMI_date
,CASE
WHEN clinical_effective_date  >= DATEADD('month', -12, p.analysis_month) AND clinical_effective_date <= p.analysis_month THEN 'Met' ELSE 'Not Met' 
END As BMI_LAST_12M
,ROW_NUMBER() OVER (PARTITION BY b.person_id, p.analysis_month ORDER BY CASE WHEN clinical_effective_date  >= DATEADD('month', -12, p.analysis_month) AND clinical_effective_date <= p.analysis_month THEN 1 ELSE 0 END DESC, clinical_effective_date desc) as row_num
FROM {{ ref('int_bmi_all') }} b
--FROM MODELLING.OLIDS_OBSERVATIONS.INT_BMI_ALL b
INNER JOIN {{ ref('int_smi_population_historical')  }} p USING (PERSON_ID)
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_HISTORICAL p USING (PERSON_ID)
where clinical_effective_date <= (select MAX(analysis_month) from {{ ref('int_smi_population_historical') }}) 
QUALIFY row_num = 1
)
--TEST 2 HBA1C OR BLOOD GLUCOSE
--latest HBA1C or Blood Glucose code that falls within the year prior to the month end date categorise as MET or NOT MET
,GLUCOSE AS (
select person_id, analysis_month, TEST_TYPE, HBA1C_LAST_12M, glucose_date
,ROW_NUMBER() OVER (PARTITION BY PERSON_ID, ANALYSIS_MONTH ORDER BY 
--if there are two tests per month prefer the one that is met
CASE WHEN HBA1C_LAST_12M = 'Met' THEN 1 ELSE 2 END 
) as rowno
from (
--HBA1c
select 
b.person_id
,p.analysis_month
,DATE(b.clinical_effective_date) as glucose_date
,'HbA1c' as TEST_TYPE
,CASE
WHEN clinical_effective_date  >= DATEADD('month', -12, p.analysis_month) AND clinical_effective_date <= p.analysis_month THEN 'Met' ELSE 'Not Met' 
END As HBA1C_LAST_12M
,ROW_NUMBER() OVER (PARTITION BY b.person_id, p.analysis_month ORDER BY CASE WHEN clinical_effective_date  >= DATEADD('month', -12, p.analysis_month) AND clinical_effective_date <= p.analysis_month THEN 1 ELSE 0 END DESC, clinical_effective_date desc) as row_num
FROM {{ ref('int_hba1c_all') }} b
--FROM MODELLING.OLIDS_OBSERVATIONS.INT_HBA1C_ALL b
INNER JOIN {{ ref('int_smi_population_historical')  }} p USING (PERSON_ID)
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_HISTORICAL p USING (PERSON_ID)
where clinical_effective_date <= (select MAX(analysis_month) from {{ ref('int_smi_population_historical') }}) 
--where clinical_effective_date <= (select MAX(analysis_month) from MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_HISTORICAL) 
QUALIFY row_num = 1

UNION
--BLOOD GLUCOSE
select 
b.person_id
,p.analysis_month
,DATE(b.clinical_effective_date) as glucose_date
,'Blood Glucose' as TEST_TYPE
,CASE
WHEN clinical_effective_date  >= DATEADD('month', -12, p.analysis_month) AND clinical_effective_date <= p.analysis_month THEN 'Met' ELSE 'Not Met' 
END As HBA1C_LAST_12M
,ROW_NUMBER() OVER (PARTITION BY b.person_id, p.analysis_month ORDER BY CASE WHEN clinical_effective_date  >= DATEADD('month', -12, p.analysis_month) AND clinical_effective_date <= p.analysis_month THEN 1 ELSE 0 END DESC, clinical_effective_date desc) as row_num
FROM {{ ref('int_blood_glucose_all') }} b
--FROM MODELLING.OLIDS_OBSERVATIONS.INT_BLOOD_GLUCOSE_ALL b
INNER JOIN {{ ref('int_smi_population_historical')  }} p USING (PERSON_ID)
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_HISTORICAL p USING (PERSON_ID)
where clinical_effective_date <= (select MAX(analysis_month) from {{ ref('int_smi_population_historical') }}) 
--where clinical_effective_date <= (select MAX(analysis_month) from MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_HISTORICAL) 
QUALIFY row_num = 1
) u
QUALIFY rowno = 1
)
--TEST 3 CHOLESTEROL OR QRISK
--latest Cholesterol code that falls within the year prior to the month end date categorise as MET or NOT MET
,CHOLESTEROL as (
select person_id, analysis_month, TEST_TYPE, CHOL_LAST_12M, Cholesterol_date
,ROW_NUMBER() OVER (PARTITION BY PERSON_ID, ANALYSIS_MONTH ORDER BY 
--if there are two tests per month prefer the one that is met
CASE WHEN CHOL_LAST_12M = 'Met' THEN 1 ELSE 2 END 
) as rowno
from (
--CHOLESTEROL
select 
c.person_id
,p.analysis_month
,DATE(c.clinical_effective_date) as Cholesterol_date
,'Cholesterol' as TEST_TYPE
,CASE
WHEN clinical_effective_date  >= DATEADD('month', -12, p.analysis_month) AND clinical_effective_date <= p.analysis_month THEN 'Met' ELSE 'Not Met' 
END As CHOL_LAST_12M
,ROW_NUMBER() OVER (PARTITION BY c.person_id, p.analysis_month ORDER BY CASE WHEN clinical_effective_date  >= DATEADD('month', -12, p.analysis_month) AND clinical_effective_date <= p.analysis_month THEN 1 ELSE 0 END DESC, clinical_effective_date desc) as row_num
FROM {{ ref('int_cholesterol_all') }} c
--FROM MODELLING.OLIDS_OBSERVATIONS.int_cholesterol_all c
INNER JOIN {{ ref('int_smi_population_historical')  }} p USING (PERSON_ID)
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_HISTORICAL p USING (PERSON_ID)
where clinical_effective_date <= (select MAX(analysis_month) from {{ ref('int_smi_population_historical') }}) 
--where clinical_effective_date <= (select MAX(analysis_month) from MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_HISTORICAL)
QUALIFY row_num = 1

UNION
--QRISK
select 
b.person_id
,p.analysis_month
,DATE(b.clinical_effective_date) as Cholesterol_date
,'QRISK' as TEST_TYPE
,CASE
WHEN clinical_effective_date  >= DATEADD('month', -12, p.analysis_month) AND clinical_effective_date <= p.analysis_month THEN 'Met' ELSE 'Not Met' 
END As HBA1C_LAST_12M
,ROW_NUMBER() OVER (PARTITION BY b.person_id, p.analysis_month ORDER BY CASE WHEN clinical_effective_date  >= DATEADD('month', -12, p.analysis_month) AND clinical_effective_date <= p.analysis_month THEN 1 ELSE 0 END DESC, clinical_effective_date desc) as row_num
FROM {{ ref('int_qrisk_all') }} b
--FROM MODELLING.OLIDS_OBSERVATIONS.INT_QRISK_ALL b
INNER JOIN {{ ref('int_smi_population_historical')  }} p USING (PERSON_ID)
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_HISTORICAL p USING (PERSON_ID)
where clinical_effective_date <= (select MAX(analysis_month) from {{ ref('int_smi_population_historical') }}) 
--where clinical_effective_date <= (select MAX(analysis_month) from MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_HISTORICAL) 
QUALIFY row_num = 1
) u
QUALIFY rowno = 1
)
--TEST 4 BLOOD PRESSURE
--latest BP code that falls within the year prior to the month end date categorise as MET or NOT MET
,BP as (
select 
bp.person_id
,p.analysis_month
,DATE(bp.clinical_effective_date) as BP_date
,CASE
WHEN clinical_effective_date  >= DATEADD('month', -12, p.analysis_month) AND clinical_effective_date <= p.analysis_month THEN 'Met' ELSE 'Not Met' 
END As BP_LAST_12M
,ROW_NUMBER() OVER (PARTITION BY bp.person_id, p.analysis_month ORDER BY CASE WHEN clinical_effective_date  >= DATEADD('month', -12, p.analysis_month) AND clinical_effective_date <= p.analysis_month THEN 1 ELSE 0 END DESC, clinical_effective_date desc) as row_num
FROM {{ ref('int_blood_pressure_all') }} bp
--FROM MODELLING.OLIDS_OBSERVATIONS.INT_BLOOD_PRESSURE_ALL bp
INNER JOIN {{ ref('int_smi_population_historical')  }} p USING (PERSON_ID)
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_HISTORICAL p USING (PERSON_ID)
where clinical_effective_date <= (select MAX(analysis_month) from {{ ref('int_smi_population_historical') }}) 
QUALIFY row_num = 1
)
-- # TEST 5 SMOKING STATUS
--latest Smoking Status that falls within the year prior to the month end date categorise as MET or NOT MET
,SMOK as (
select 
s.person_id
,p.analysis_month
,DATE(s.clinical_effective_date) as SMOK_date
,CASE
WHEN clinical_effective_date  >= DATEADD('month', -12, p.analysis_month) AND clinical_effective_date <= p.analysis_month THEN 'Met' ELSE 'Not Met' 
END As SMOK_LAST_12M
,ROW_NUMBER() OVER (PARTITION BY s.person_id, p.analysis_month ORDER BY CASE WHEN clinical_effective_date  >= DATEADD('month', -12, p.analysis_month) AND clinical_effective_date <= p.analysis_month THEN 1 ELSE 0 END DESC, clinical_effective_date desc) as row_num
FROM {{ ref('int_smoking_status_all') }} s
--FROM MODELLING.OLIDS_PERSON_ATTRIBUTES.INT_SMOKING_STATUS_ALL s
INNER JOIN {{ ref('int_smi_population_historical')  }} p USING (PERSON_ID)
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_HISTORICAL p USING (PERSON_ID)
where clinical_effective_date <= (select MAX(analysis_month) from {{ ref('int_smi_population_historical') }}) 
QUALIFY row_num = 1
)

--TEST # 6 ALCOHOL ASSESSMENT EITHER AUDIT C or UNITS PER WEEK OR ALCOHOL DISORDER. 
,ALC AS (
--AUDIT SCORES
WITH all_audit_scores AS (
    select
    person_id
    ,analysis_month
    ,audit_date as assessment_date
    ,audit_type as assessment_type
    ,ASSESSED_LAST_12M
,ROW_NUMBER() OVER (PARTITION BY person_id, analysis_month ORDER BY CASE WHEN assessment_date  >= DATEADD('month', -12, analysis_month) AND assessment_date <= analysis_month THEN 1 ELSE 0 END DESC, assessment_date desc) as row_num
    FROM (
    -- Get the all AUDIT score for each person
    SELECT distinct
        a.person_id
        ,p.analysis_month
        ,DATE(a.clinical_effective_date) AS audit_date
        ,CASE
    WHEN clinical_effective_date  >= DATEADD('month', -12, p.analysis_month) AND clinical_effective_date <= p.analysis_month THEN 'Met' ELSE 'Not Met' 
    END As ASSESSED_LAST_12M
        ,a.audit_type
         -- Prefer full AUDIT if same date
        ,ROW_NUMBER() OVER (PARTITION BY person_id, clinical_effective_date ORDER BY CASE WHEN audit_type = 'Full AUDIT' THEN 1 ELSE 2 END ) AS rn            
        FROM {{ ref('int_alcohol_audit_scores') }} a
        --FROM MODELLING.OLIDS_OBSERVATIONS.INT_ALCOHOL_AUDIT_SCORES a
        INNER JOIN {{ ref('int_smi_population_historical')  }} p USING (PERSON_ID)
        --INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_HISTORICAL p USING (PERSON_ID)
        WHERE is_valid_score = TRUE 
        AND clinical_effective_date <= (select MAX(analysis_month) from {{ ref('int_smi_population_historical') }}) 
        QUALIFY rn = 1               
    ) a
     QUALIFY row_num = 1
     )     
     --ALCOHOL DISORDERS
 ,all_alcohol_disorders AS (
  select
    person_id
    ,analysis_month
    ,disorder_date as assessment_date
    ,'Alcohol Misuse Disorder' as assessment_type
    ,ASSESSED_LAST_12M
,ROW_NUMBER() OVER (PARTITION BY person_id, analysis_month ORDER BY CASE WHEN assessment_date  >= DATEADD('month', -12, analysis_month) AND assessment_date <= analysis_month THEN 1 ELSE 0 END DESC, assessment_date desc) as row_num
    FROM (
    -- Get alcohol disorder history - basic summary- as active and historical
    SELECT DISTINCT
        d.person_id
        ,p.analysis_month
       ,d.concept_display
       ,DATE(d.clinical_effective_date) AS disorder_date
    ,CASE 
    WHEN clinical_effective_date >= DATEADD('month', -12, p.analysis_month) AND clinical_effective_date <= p.analysis_month THEN 'Met' ELSE 'Not Met' 
    END As ASSESSED_LAST_12M
    FROM {{ ref('int_alcohol_misuse_disorders') }} d
    --FROM MODELLING.OLIDS_OBSERVATIONS.int_alcohol_misuse_disorders d
    INNER JOIN {{ ref('int_smi_population_historical')  }} p USING (PERSON_ID)
    --INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_HISTORICAL p USING (PERSON_ID)
    where clinical_effective_date <= (select MAX(analysis_month) from {{ ref('int_smi_population_historical') }}) 
      ) a
   QUALIFY row_num = 1
    )
--ALCOHOL UNITS 
,all_alcohol_units as (
 select
    person_id
    ,analysis_month
    ,alcohol_assessment_date as assessment_date
    ,'Alcohol Unit Assessment' as assessment_type
    ,ASSESSED_LAST_12M
,ROW_NUMBER() OVER (PARTITION BY person_id, analysis_month ORDER BY CASE WHEN assessment_date  >= DATEADD('month', -12, analysis_month) AND assessment_date <= analysis_month THEN 1 ELSE 0 END DESC, assessment_date desc) as row_num
    FROM (
    --get all unit value assessments using ALC_COD
    select distinct
    a.person_id
    ,p.analysis_month
    ,a.clinical_effective_date as alcohol_assessment_date
     ,CASE 
     WHEN clinical_effective_date >= DATEADD('month', -12, p.analysis_month) AND clinical_effective_date <= p.analysis_month THEN 'Met' ELSE 'Not Met' 
     END As ASSESSED_LAST_12M
    ,a.result_value as alcohol_units
    ,a.result_unit_display as unit_display
    FROM {{ ref('int_alcohol_units_all') }} a
    --FROM MODELLING.OLIDS_OBSERVATIONS.int_smi_alcohol_all a
    INNER JOIN {{ ref('int_smi_population_historical')  }} p USING (PERSON_ID)
    --INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_HISTORICAL p USING (PERSON_ID)
    where clinical_effective_date <= (select MAX(analysis_month) from {{ ref('int_smi_population_historical') }}) 
   ) a
     QUALIFY row_num = 1
    )
  
,combined AS (
    SELECT PERSON_ID, ANALYSIS_MONTH, ASSESSMENT_DATE, ASSESSMENT_TYPE, ASSESSED_LAST_12M 
    FROM all_audit_scores
    UNION ALL
    SELECT PERSON_ID, ANALYSIS_MONTH, ASSESSMENT_DATE, ASSESSMENT_TYPE, ASSESSED_LAST_12M   
    FROM all_alcohol_disorders
    UNION ALL
    SELECT PERSON_ID, ANALYSIS_MONTH, ASSESSMENT_DATE, ASSESSMENT_TYPE, ASSESSED_LAST_12M  
    FROM all_alcohol_units
)
-- Pick the latest assessment per person + month for all alcohol assessments combined - preference for AUDIT over units
SELECT 
    PERSON_ID
    ,ANALYSIS_MONTH
    ,ASSESSMENT_DATE
    ,ASSESSMENT_TYPE
    ,ASSESSED_LAST_12M AS ALC_LAST_12M
     -- tie-breaker if same date select AUDIT over alcohol units
    ,ROW_NUMBER() OVER (PARTITION BY PERSON_ID, ANALYSIS_MONTH ORDER BY ASSESSMENT_DATE DESC, 
CASE WHEN ASSESSMENT_TYPE <> 'Alcohol Unit Assessment' THEN 1 ELSE 2 END , ASSESSMENT_TYPE
) as rowno
FROM combined
QUALIFY rowno = 1
)
,HC_CHECKS AS (
SELECT 
        p.person_id
        ,p.analysis_month
        ,p.has_smi
        ,p.fiscal_year_label
        ,p.PRACTICE_NAME
        ,p.PRACTICE_CODE
        ,p.practice_borough
        ,p.primary_care_network
        ,CASE WHEN b.person_id IS NULL THEN 'Not Met' ELSE b.BMI_LAST_12M END AS BMI_CHECK_12M
        ,CASE WHEN g.person_id IS NULL THEN 'Not Met' ELSE g.HBA1C_LAST_12M END AS HBA1C_CHECK_12M
        ,CASE WHEN c.person_id IS NULL THEN 'Not Met' ELSE c.CHOL_LAST_12M END AS CHOL_CHECK_12M
        ,CASE WHEN bp.person_id IS NULL THEN 'Not Met' ELSE bp.BP_LAST_12M END AS BP_CHECK_12M
        ,CASE WHEN s.person_id IS NULL THEN 'Not Met' ELSE s.SMOK_LAST_12M END AS SMOK_CHECK_12M
        ,CASE WHEN a.person_id IS NULL THEN 'Not Met' ELSE a.ALC_LAST_12M END AS ALC_CHECK_12M
        ,p.gender
        ,p.age_band_nhs
        ,p.age_nhs_order
        ,p.ethnicity_category
        ,p.ethcat_order
        ,p.ethnicity_subcategory
        ,p.ethsubcat_order
        ,p.imd_quintile
        ,p.imdquintile_order
    --FROM MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_HISTORICAL p
    FROM {{ ref('int_smi_population_historical')  }} p
    LEFT JOIN BMI b on b.person_id = p.person_id AND b.analysis_month = p.analysis_month
    LEFT JOIN GLUCOSE g on g.person_id = p.person_id AND g.analysis_month = p.analysis_month
    LEFT JOIN CHOLESTEROL c on c.person_id = p.person_id AND c.analysis_month = p.analysis_month
    LEFT JOIN BP bp on bp.person_id = p.person_id AND bp.analysis_month = p.analysis_month
    LEFT JOIN SMOK s on s.person_id = p.person_id AND s.analysis_month = p.analysis_month
   LEFT JOIN ALC a on a.person_id = p.person_id AND a.analysis_month = p.analysis_month
)
,MET_COUNT as (
SELECT DISTINCT
  PERSON_ID
  ,ANALYSIS_MONTH
  ,((BMI_CHECK_12M      = 'Met')::int +
        (HBA1C_CHECK_12M    = 'Met')::int +
        (CHOL_CHECK_12M     = 'Met')::int +
        (BP_CHECK_12M       = 'Met')::int +
        (SMOK_CHECK_12M      = 'Met')::int +
        (ALC_CHECK_12M      = 'Met')::int
    ) AS MET_COUNT
FROM HC_CHECKS
GROUP by ALL
)
--FINAL OUTPUT
SELECT hc.*,
NVL(mc.MET_COUNT,0) AS MET_CT
FROM HC_CHECKS hc
left JOIN MET_COUNT mc on hc.person_id = mc.person_id and hc.analysis_month = mc.analysis_month