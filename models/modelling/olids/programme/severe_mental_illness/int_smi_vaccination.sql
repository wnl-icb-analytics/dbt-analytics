{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['smi_registry']
        )
}}
--SMI VACCINATION STATUS COVID AND FLU - LONG TABLE MULTIPLE ROWS PER PERSON
--flu if eligible (morbidly obese etc) and covid (should be age 75+, Imm supp and care home only)
WITH FLU AS (
select 
  s.person_id
  ,s.age
  ,f.campaign_id as vaccination_type
  ,CASE 
  WHEN f.campaign_id ='Flu 2025-26' and f.person_id is not null then TRUE else FALSE end as ELIGIBLE
  ,CASE 
  WHEN f.campaign_id ='Flu 2025-26' and f.person_id is not null THEN f.VACCINATION_STATUS END AS VACCINATION_STATUS
   ,CASE 
  WHEN f.campaign_id ='Flu 2025-26' and f.person_id is not null THEN DATE(f.VACCINATION_DATE) END AS VACCINATION_DATE
  ,LISTAGG(DISTINCT f.risk_group, ', ') as vaccine_group
  --FROM MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE s
  FROM {{ ref('int_smi_population_base')  }} s
  --LEFT JOIN PUBLISHED_REPORTING__DIRECT_CARE.OLIDS_COVID_FLU.COVID_FLU_DASHBOARD_BASE f using (person_id)
  LEFT JOIN {{ ref('covid_flu_dashboard_base') }} f using (person_id)
  WHERE F.campaign_id in ('Flu 2025-26') and F.is_active = true
  group by all
  )
  ,COVID AS (
  select 
  s.person_id
  ,s.age
  ,c.campaign_id as vaccination_type
  ,CASE 
  WHEN c.campaign_id ='COVID Autumn 2025' and c.person_id is not null then TRUE else FALSE end as ELIGIBLE
 ,CASE 
  WHEN c.campaign_id ='COVID Autumn 2025' and c.person_id is not null THEN c.VACCINATION_STATUS END AS VACCINATION_STATUS
  ,CASE 
  WHEN c.campaign_id ='COVID Autumn 2025' and c.person_id is not null THEN DATE(c.VACCINATION_DATE) END AS VACCINATION_DATE
  ,LISTAGG(DISTINCT c.risk_group, ', ') as vaccine_group
  FROM {{ ref('int_smi_population_base')  }} s
  --FROM MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE s
  LEFT JOIN {{ ref('covid_flu_dashboard_base') }} c using (person_id)
  --LEFT JOIN PUBLISHED_REPORTING__DIRECT_CARE.OLIDS_COVID_FLU.COVID_FLU_DASHBOARD_BASE c using (person_id)
 WHERE C.campaign_id in ('COVID Autumn 2025') and c.is_active = true
 group by all
)
--Age 65+ or Clinical Risk PPV group
,PPV as (
 select 
  s.person_id
  ,s.age
  ,ppv.campaign AS vaccination_type
  ,CASE 
  WHEN ppv.campaign IS NOT NULL THEN TRUE ELSE FALSE END AS ELIGIBLE
 ,CASE 
  WHEN ppv.campaign IS NOT NULL and ppv.person_id is not null THEN ppv.VACCINATION_STATUS END AS VACCINATION_STATUS
  ,CASE 
  WHEN ppv.campaign IS NOT NULL and ppv.person_id is not null THEN DATE(ppv.VACCINATION_DATE) END AS VACCINATION_DATE
  ,case when IN_PPV_CLINICAL_RISK_GROUP THEN 'Clinical Risk PPV' ELSE '65+ Routine' END as vaccine_group
  FROM {{ ref('int_smi_population_base')  }} s
  --FROM MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE s
  LEFT JOIN {{ ref('fct_pneumococcal_vaccination_status') }} ppv using (person_id)
  --LEFT JOIN REPORTING.OLIDS_PROGRAMME.FCT_PNEUMOCOCCAL_VACCINATION_STATUS ppv using (person_id)
  WHERE ppv.person_id is not null
)
--age 75+ , Care Home or Pregnancy.
,RSV as (
select 
  s.person_id
  ,s.age
  ,r.campaign AS vaccination_type
  ,CASE 
  WHEN r.campaign IS NOT NULL THEN TRUE ELSE FALSE END AS ELIGIBLE
 ,CASE 
  WHEN r.campaign IS NOT NULL and r.person_id is not null THEN r.VACCINATION_STATUS END AS VACCINATION_STATUS
  ,CASE 
  WHEN r.campaign IS NOT NULL and r.person_id is not null THEN DATE(r.VACCINATION_DATE) END AS VACCINATION_DATE
   ,CASE 
  WHEN IS_PREGNANT THEN 'Currently Pregnant'
  WHEN IS_CARE_HOME_RESIDENT THEN 'Care Home Resident'
  ELSE '75+ Routine' END as vaccine_group
  FROM {{ ref('int_smi_population_base')  }} s
  --FROM MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE s
  LEFT JOIN {{ ref('fct_rsv_vaccination_status') }} r using (person_id)
  --LEFT JOIN REPORTING.OLIDS_PROGRAMME.FCT_RSV_VACCINATION_STATUS r using (person_id)
  WHERE r.person_id is not null
)
--Age 65+ or Clinical Risk PPV group
,SHING as (
select 
  s.person_id
  ,s.age
  ,sh.campaign AS vaccination_type
  ,CASE 
  WHEN sh.campaign IS NOT NULL THEN TRUE ELSE FALSE END AS ELIGIBLE
 ,CASE 
  WHEN sh.campaign IS NOT NULL and sh.person_id is not null THEN sh.VACCINATION_STATUS END AS VACCINATION_STATUS
  ,CASE 
  WHEN sh.campaign IS NOT NULL and sh.person_id is not null THEN DATE(sh.VACCINATION_DATE) END AS VACCINATION_DATE
   ,CASE 
  WHEN is_immunosuppressed THEN 'Immunosuppressed' ELSE '65+ Routine' END as vaccine_group
  FROM {{ ref('int_smi_population_base')  }} s
  --FROM MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE s
  LEFT JOIN {{ ref('fct_shingles_vaccination_status') }} sh using (person_id)
  --LEFT JOIN REPORTING.OLIDS_PROGRAMME.FCT_SHINGLES_VACCINATION_STATUS sh using (person_id)
  WHERE sh.person_id is not null
)
,COMBINED as (
Select * 
FROM (
select f.*
from FLU f

UNION

select c.*
from COVID c

UNION
select p.*
from PPV p

UNION
select r.*
from RSV r

UNION
select s.*
from SHING s
)
order by 1
)

--Population demographics + Vaccination metrics long table
select
p.PERSON_ID
,p.HX_FLAKE
,p.PRACTICE_CODE
,p.practice_name
,p.RESIDENTIAL_LOC
,p.RESIDENTIAL_BOROUGH
,p.AGE
,p.BIRTH_DATE_APPROX
,p.AGE_BAND_5Y
,p.AGE_BAND_NHS
,p.GENDER
,p.ETHNICITY_CATEGORY
,p.ETHCAT_ORDER
,p.ETHNICITY_SUBCATEGORY
,p.ETHSUBCAT_ORDER
,p.IMD_QUINTILE
,p.IMDQUINTILE_ORDER
,p.MAIN_LANGUAGE
,p.is_homeless
,c.vaccination_type 
,c.vaccine_group
,c.eligible
,c.vaccination_status
,c.vaccination_date
--FROM MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p
FROM {{ ref('int_smi_population_base')  }} p
LEFT JOIN COMBINED c using (person_id)
WHERE p.HAS_ACTIVE_SMI_DIAGNOSIS