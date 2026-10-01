{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['smi_registry']
        )
}}
--FIND ENHANCED LTC METRICS - WIDE TABLE - PERSON LEVEL
--summarise all QOF LTCS exlcuding SMI,OB,Pallitive care PC and NDH (non diabetic hypo)
with ltc_sum as (
SELECT
  l.PERSON_ID
  ,LISTAGG(DISTINCT CONDITION_CODE, ',') AS ltc_summary
   ,COUNT(DISTINCT CONDITION_CODE) AS ltc_count
--FROM REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_LTC_SUMMARY
FROM {{ ref('fct_person_ltc_summary')  }} l
INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p USING (PERSON_ID)
WHERE IS_QOF = TRUE AND CONDITION_CODE is not null AND condition_code not in ('OB','SMI','NDH','PC')
GROUP BY l.PERSON_ID
)
--prescribed antipyschotic drugs or depot injections within last 6 months
,antipyschotics as (
    SELECT
        p.person_id,
        MAX(ap.order_date) AS latest_antipsychotic_order_date,
        MIN(
            CASE WHEN ap.order_date >= CURRENT_DATE - INTERVAL '6 months' THEN ap.order_date END
        ) AS earliest_recent_antipsychotic_date,
        COUNT(
            CASE WHEN ap.order_date >= CURRENT_DATE - INTERVAL '6 months' THEN 1 END
        ) AS recent_antipsychotic_orders_count,
        ARRAY_AGG(DISTINCT ap.mapped_concept_code)
            AS all_antipsychotic_concept_codes,
        ARRAY_AGG(DISTINCT ap.mapped_concept_display)
            AS all_antipsychotic_concept_displays
    FROM {{ ref('int_antipsychotic_medications_all') }} AS ap
    --FROM DEV__MODELLING.OLIDS_MEDICATIONS.INT_ANTIPSYCHOTIC_MEDICATIONS_ALL ap 
    INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)
    --INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p USING (PERSON_ID)
       WHERE ap.order_date >= CURRENT_DATE - INTERVAL '6 months'  -- Only recent antipsychotic orders
    GROUP BY p.person_id
    )
--CARE PLAN
,latest_Care as (
select 
m.person_id
,DATE(m.clinical_effective_date) as MH_CARE_PLAN_DATE
,m.MH_CARE_PLAN_CURRENT_12M
--FROM DEV__MODELLING.OLIDS_OBSERVATIONS.INT_SMI_CARE_PLAN_LATEST m
FROM {{ ref('int_smi_care_plan_latest')  }} m
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p USING (PERSON_ID)
INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)
)
--MEDICATION REVIEW
,MED_REV as (
select 
mr.person_id
,DATE(mr.clinical_effective_date) as MED_REVIEW_DATE
,mr.MED_REV_LAST_12M
--FROM DEV__MODELLING.OLIDS_OBSERVATIONS.INT_SMI_LONGLIVES_MED_REVIEW_LATEST mr
FROM {{ ref('int_medication_review_latest')  }} mr
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p USING (PERSON_ID)
INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)
)
--QRISK
,qrisk as (
select 
q.person_id
,DATE(q.clinical_effective_date) as QRISK_DATE
,q.cvd_risk_category
--FROM DEV__MODELLING.OLIDS_OBSERVATIONS.int_qrisk_latest q
FROM {{ ref('int_qrisk_latest')  }} q
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p USING (PERSON_ID)
INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)
)
--with Total cholesterol_CHOL healthy heart <5 mmol/L
,TCHOL_HH as (
select 
c.person_id
,DATE(c.clinical_effective_date) as TOTAL_CHOL_DATE
,c.CHOLESTEROL_VALUE as TOTAL_CHOL_VALUE
,CASE 
WHEN CHOLESTEROL_VALUE >= 0.5 AND CHOLESTEROL_VALUE < 5.0 THEN 'Met' 
WHEN CHOLESTEROL_VALUE BETWEEN 5.0 AND 20.0 THEN 'Not Met'
ELSE 'OTHER'
END AS TC_UNDER_FIVE_TARGET
--FROM MODELLING.OLIDS_OBSERVATIONS.int_cholesterol_latest c
FROM {{ ref('int_cholesterol_latest') }} c
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p USING (PERSON_ID)
INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)
)
--LDL target for those with CHD or STIA or PAD
,LDLCHOL as (
select 
c.person_id
,p.has_chd
,p.has_pad
,p.has_stia
,DATE(c.clinical_effective_date) as LDL_CHOL_DATE
,c.CHOLESTEROL_VALUE as LDL_CHOL_VALUE
,IFF(
  COALESCE(p.has_chd, FALSE)
  OR COALESCE(p.has_pad, FALSE)
  OR COALESCE(p.has_stia, FALSE),
  c.LDL_CVD_TARGET_MET, NULL
) AS LDL_CVD_TARGET_MET
--FROM MODELLING.OLIDS_OBSERVATIONS.int_cholesterol_LDL_latest c
FROM {{ ref('int_cholesterol_ldl_latest') }} c
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p USING (PERSON_ID)
INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)
)
--NDPP
,NDPP as (
select 
n.person_id
,n.clinical_effective_date as NDPP_DATE
,CASE
WHEN n.concept_code = '1090701000000104' THEN 'NDPP invite'
WHEN n.concept_code = '1025321000000109' THEN 'NDPP referral'
WHEN n.concept_code = '1025301000000100' THEN 'NDPP declined'
END AS NDPP_STATUS
--FROM MODELLING.OLIDS_PROGRAMME.INT_REFERRAL_NDPP_LATEST n
FROM {{ ref('int_referral_ndpp_latest') }} n
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p USING (PERSON_ID)
INNER JOIN {{ ref('int_smi_population_base')  }} p USING (PERSON_ID)
)
--Has diabetes and DIABTES 8 CARE PROCESSES and TRIPLE TARGET
,diabetes as (
select 
p.person_id
,d3.ALL_THREE_TARGETS_MET AS DM_TRIPLE_TARGET_MET
,d8.CARE_PROCESSES_COMPLETED AS DM_EIGHT_CARE_PROCESSES
--FROM MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p 
FROM {{ ref('int_smi_population_base')  }} p 
--LEFT JOIN REPORTING.OLIDS_MEASURES.FCT_PERSON_DIABETES_TRIPLE_TARGET d3 using (person_id)
LEFT JOIN {{ ref('fct_person_diabetes_triple_target') }} d3 using (person_id)
--LEFT JOIN REPORTING.OLIDS_MEASURES.FCT_PERSON_DIABETES_8_CARE_PROCESSES d8 using (person_id)
LEFT JOIN {{ ref('fct_person_diabetes_8_care_processes') }} d8 using (person_id)
where p.has_diabetes = 'TRUE'
)
--has hypertension and BP is controlled
,hyp as (
select 
p.person_id
,h.IS_OVERALL_BP_CONTROLLED AS HYP_BP_CONTROLLED
--FROM MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p 
FROM {{ ref('int_smi_population_base')  }} p 
--LEFT JOIN REPORTING.OLIDS_MEASURES.FCT_PERSON_BP_CONTROL h using (person_id)
LEFT JOIN {{ ref('fct_person_bp_control') }} h using (person_id)
where p.has_hyp = 'TRUE'
)
--Pulmonary Rehab
,PUL_REHAB as (
select 
pr.person_id
,pr.clinical_effective_date as PUL_REHAB_DATE
,pr.pr_obs_type as PUL_REHAB_STATUS
--FROM MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p 
FROM {{ ref('int_smi_population_base')  }} p 
--INNER JOIN MODELLING.OLIDS_OBSERVATIONS.INT_REFERRAL_PULMONARY_REHAB_LATEST pr using (PERSON_ID)
LEFT JOIN {{ ref('int_referral_pulmonary_rehab_latest') }} pr using (PERSON_ID)
)

--Population demographics + enhanced LTC metrics wide table
select 
p.PERSON_ID
,p.HX_FLAKE
,p.AGE
,p.GENDER
,p.HAS_ACTIVE_SMI_DIAGNOSIS
,p.IS_ON_LITHIUM
,CASE WHEN ap.PERSON_ID IS NOT NULL THEN TRUE END AS IS_ON_ANTIPSYCHOTICS 
,m.MH_CARE_PLAN_DATE
,m.MH_CARE_PLAN_CURRENT_12M
,mr.MED_REVIEW_DATE
,mr.MED_REV_LAST_12M
,ltc.LTC_COUNT
,ltc.LTC_SUMMARY
--CVD
,h.HYP_BP_CONTROLLED
,q.QRISK_DATE
,q.CVD_RISK_CATEGORY
,tc.TOTAL_CHOL_DATE
,tc.TOTAL_CHOL_VALUE
,tc.TC_UNDER_FIVE_TARGET
,lc.LDL_CHOL_DATE
,lc.LDL_CHOL_VALUE
,lc.LDL_CVD_TARGET_MET
--METABOLIC
,n.NDPP_DATE
,n.NDPP_STATUS
,d.DM_TRIPLE_TARGET_MET
,d.DM_EIGHT_CARE_PROCESSES
--respiratory
,pr.PUL_REHAB_DATE
,pr.PUL_REHAB_STATUS
--FROM MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p
FROM {{ ref('int_smi_population_base')  }} p
LEFT JOIN LTC_SUM ltc using (person_id)
LEFT JOIN antipyschotics ap using (person_id) 
LEFT JOIN latest_Care m using (person_id)
LEFT JOIN qrisk q using (person_id)
LEFT JOIN TCHOL_HH tc using (person_id)
LEFT JOIN LDLCHOL lc using (person_id)
LEFT JOIN HYP h using (person_id)
LEFT JOIN MED_REV mr using (person_id)
LEFT JOIN Diabetes d using (person_id)
LEFT JOIN ndpp n using (person_id)
LEFT JOIN PUL_REHAB pr using (person_id)