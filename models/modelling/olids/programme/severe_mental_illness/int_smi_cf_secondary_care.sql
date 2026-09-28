{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['smi_registry']
        )
}}
/*Person Level Intermediate table holding Case Finding data for people on the SMI register who have had an inpatient spell at NLFT in the last 6 months or who have a current inpatient spell. 
This includes number of checks missed and vulnerabilities. The MHSDS active submission is 6 weeks behind current date.
*/

-- Find the latest provider-local identifier for each MHSDS person and provider.
-- MHS001 is monthly history; the bridge supplies the cross-system patient key.
with mpi_latest as (
    select *
    from {{ ref('stg_mhsds_mpi_history') }}
    -- restrict to NLFT before ranking: org_id_prov is part of the partition,
    -- so other providers form separate partitions this model never uses
    where org_id_prov in ('G6V2S')
    qualify row_number() over (
        partition by person_id, org_id_prov
        order by reporting_period_end_date desc, mhs001_uniq_id desc
    ) = 1
)

, LOCAL_ID as (
select distinct
b.sk_patient_id
,mpi.PERSON_ID as mpi_person_id
,local_patient_id
,org_id_prov
,'NLFT' as provider
,mpi.reporting_period_end_date as latest_reporting_date
FROM mpi_latest mpi
INNER JOIN {{ ref('stg_mhsds_bridging') }} b ON mpi.person_id = b.person_id
-- INNER JOIN STAGING.MHSDS.STG_MHSDS_BRIDGING b ON mpi.person_id = b.person_id
INNER JOIN {{ ref('int_smi_population_base') }} smi on smi.sk_patient_id = TO_NUMBER(b.sk_patient_id)
-- INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE smi on smi.sk_patient_id = TO_NUMBER(b.sk_patient_id)
where ORG_ID_PROV = 'G6V2S' --use NLFT code only C&I legacy patients are not found in the NLFT EPR system
and mpi.DMIC_CCG_CODE = '93C'
and smi.HAS_ACTIVE_SMI_DIAGNOSIS
and mpi.pers_death_date is null -- extra check to exclude people who have died as they will not be in the EPR system and therefore will not have case finding data. This is in addition to the death date check in the population base definition.
)
--ward code look up - use reporting fct table
,WARD_DETAILS AS (
select distinct ward_code, site_name 
from (
select distinct ward_code, WARD_SITE_CODE,
CASE 
WHEN WARD_SITE_CODE = 'A0G9K' THEN 'Edgware'
WHEN WARD_SITE_CODE = 'A3D2M' THEN 'St Anns'
WHEN WARD_SITE_CODE = 'A3C5P' THEN 'Barnet'
WHEN WARD_SITE_CODE = 'A5E8R' THEN 'Highgate'
WHEN WARD_SITE_CODE in ('A1D5T','A1X5K') AND WARD_CODE <> 'FXAvew' THEN 'Chase Farm'
WHEN WARD_SITE_CODE = 'A1D5T' and ward_code in ('FXAvew') THEN 'Avesbury'
WHEN WARD_SITE_CODE is null and ward_code = 'CumbriaFX' THEN 'Chase Farm'
WHEN WARD_SITE_CODE is null and ward_code = 'HCPH' THEN 'Haringey'
ELSE 'Unknown' END AS site_name
--FROM REPORTING.MENTAL_HEALTH.FCT_MHSDS_WARD_STAY
FROM {{ ref('fct_mhsds_ward_stay') }}
where provider_organisation_code = 'G6V2S'
) a
)
--DEFINE SMI POP
,SMIPOPULATION as (
SELECT DISTINCT
smi.sk_patient_id
,b.mpi_person_id
,smi.person_id
,smi.hx_flake
,smi.age
,smi.age_band_5y
,smi.gender
,smi.birth_date_approx
,smi.ethnicity_category
,smi.ethcat_order
,smi.ethnicity_subcategory
,smi.ethsubcat_order
,smi.ethnicity_granular
,smi.practice_code
,smi.practice_name
,smi.main_language
,IFF(smi.is_homeless, 'Yes', 'No') AS is_homeless
,smi.imd_quintile
,smi.imdquintile_order
,IFF(smi.interpreter_needed, 'Yes', 'No') AS interpreter_needed
,smi.interpreter_type
,cf.is_smoker
,cf.drug_use
,cf.alcohol_use
,cf.ltc_count
,cf.ltc_2plus
,cf.ltc_summary
,cf.is_on_lithium
--health_check_completed
,cf.has_declined
,cf.incomp12m_ct
,cf.incomp12m_list
,cf.smok_miss
,cf.alc_miss
,cf.bp_miss
,cf.chol_miss
,cf.bmi_miss
,cf.hba1c_miss
FROM {{ ref('int_smi_population_base') }} smi
--FROM MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE smi
--add in case finding data for people on the SMI register
LEFT JOIN {{ ref('int_smi_casefinding') }} cf on smi.person_id = cf.person_id
--LEFT JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_CASEFINDING cf on smi.person_id = cf.person_id
--Include any person who has had activity at NLFT.
INNER JOIN (SELECT DISTINCT mpi_person_id, sk_patient_id FROM LOCAL_ID) b ON smi.sk_patient_id = b.sk_patient_id
WHERE HAS_ACTIVE_SMI_DIAGNOSIS
)
--Inpatient stays that started in the last 6 months or are currently active for people on the SMI register. Some people have multiple spells and ward stays, so we select the latest ward stay only.
--September 2026 switch to FCT_MHSDS analyst tables in reporting layer.
,SPELL as (
    select distinct 
    p.sk_patient_id
    ,p.mpi_person_id
    ,'NLFT' as provider
    ,sp.source_record_id as spell_number
    ,ws.uniq_ward_stay_id
    ,ws.ward_code
    ,wd.site_name
    ,DATE(sp.admission_date) as spell_start_date
    ,DATE(sp.discharge_date) spell_discharge_date
    ,sp.discharge_date is null as is_current_spell 
    ,sp.source_spell_status
    ,DATE(ws.WARD_STAY_START_DATE)  as start_date_ward_stay
    ,DATE(ws.WARD_STAY_END_DATE) as end_date_ward_stay
    ,CASE WHEN sp.admission_date  >= DATEADD('month', -6, CURRENT_DATE) THEN 'Yes' ELSE 'No' END AS last_6mths_flag
    ,CASE
    WHEN sp.admission_method_code in ('11','12','13') THEN 'Elective'
    WHEN sp.admission_method_code in ('81') THEN 'Other transfer'
    ELSE 'Emergency' END AS admission_type
     ,CASE 
    WHEN ws.SOURCE_DERIVED_HOSPITAL_BED_TYPE_NAME = 'Adult Psychiatric Intensive Care Unit (Acute Mental Health Care)' THEN 'Adult Psychiatric Intensive Care Unit'
    WHEN ws.SOURCE_DERIVED_HOSPITAL_BED_TYPE_NAME = 'Acute Older Adult Mental Health Care (Organic and Functional)' THEN 'Acute Older Adult Mental Health Care'
    WHEN ws.SOURCE_DERIVED_HOSPITAL_BED_TYPE_NAME = 'Adult Mental Health Rehabilitation (Mainstream Service)' THEN 'Adult Mental Health Rehabilitation'
    WHEN ws.SOURCE_DERIVED_HOSPITAL_BED_TYPE_NAME = 'General Child and Young Person - Young Person (13 years up to and including 17 years)' THEN 'General Child and Young Person'
    ELSE ws.SOURCE_DERIVED_HOSPITAL_BED_TYPE_NAME END AS ward_type
FROM {{ ref('fct_mhsds_hospital_provider_spell') }} sp
--FROM REPORTING.MENTAL_HEALTH.FCT_MHSDS_HOSPITAL_PROVIDER_SPELL sp
INNER JOIN SMIPOPULATION p ON p.mpi_person_id = sp.person_id
LEFT JOIN {{ ref('fct_mhsds_ward_stay') }} ws on sp.source_record_id = ws.RECORDED_HOSPITAL_PROVIDER_SPELL_ID
--LEFT JOIN REPORTING.MENTAL_HEALTH.FCT_MHSDS_WARD_STAY ws on sp.source_record_id = ws.RECORDED_HOSPITAL_PROVIDER_SPELL_ID
LEFT JOIN WARD_DETAILS wd on ws.ward_code = wd.ward_code
WHERE sp.source_derived_icb_commissioner_code = '93C'
and sp.provider_organisation_code = 'G6V2S' --use NLFT code only C&I legacy patients are not found in the NLFT EPR system
--deduplicate selecting latest ward_start_date only
QUALIFY ROW_NUMBER() OVER (PARTITION BY sp.person_id, sp.source_record_id ORDER BY start_date_ward_stay DESC) = 1
)
--select people who are inpatients currently or who have been admitted in the last 6 months
,SPELL_6M AS (
select 
sk_patient_id
,mpi_person_id
,provider
,uniq_ward_stay_id
,admission_type
,ward_type
,ward_code
,site_name
,spell_number
,spell_start_date
,start_date_ward_stay
,spell_discharge_date
,is_current_spell
,last_6mths_flag
FROM SPELL
where last_6mths_flag = 'Yes' OR is_current_spell
)
--select latest spell if multiple reported.
,latest_spell as (
select *
from spell_6m sp
QUALIFY ROW_NUMBER() OVER (PARTITION BY sp.sk_patient_id ORDER BY spell_start_date DESC, start_date_ward_stay DESC) = 1
)
--final add back in population characteristics and health check flags and local patient id for NFLT.
select 
p.person_id
--add in 2 IDs for testing purposes only. sk_patient_id and sp.mpi_person_id. REMOVE THESE FOR FINAL OUTPUT IN VIEW
,sp.sk_patient_id
,sp.mpi_person_id
,p.hx_flake
,loc.local_patient_id 
,loc.latest_reporting_date
,sp.spell_number
,sp.spell_start_date
,sp.spell_discharge_date
,sp.is_current_spell
,sp.admission_type
,sp.ward_type
,sp.ward_code
,sp.site_name
,p.age
,p.age_band_5y
,p.gender
,p.birth_date_approx
,p.ethnicity_category
,p.ethcat_order
,p.ethnicity_subcategory
,p.ethsubcat_order
,p.ethnicity_granular
,p.practice_code
,p.practice_name
,p.main_language
,p.is_homeless
,p.imd_quintile
,p.imdquintile_order
,p.interpreter_needed
,p.interpreter_type
,p.is_smoker
,p.drug_use
,p.alcohol_use
,p.ltc_count
,p.ltc_2plus
,p.ltc_summary
,p.is_on_lithium
--health_check_completed
,p.has_declined
,p.incomp12m_ct
,p.incomp12m_list
,p.smok_miss
,p.alc_miss
,p.bp_miss
,p.chol_miss
,p.bmi_miss
,p.hba1c_miss
from  latest_spell sp
left join smipopulation p on p.mpi_person_id = sp.mpi_person_id
--some people have multiple MPI_PERSON_IDs to each sk_patient_id/local_patient_id.
left join (select distinct mpi_person_id, local_patient_id, latest_reporting_date from LOCAL_ID) loc on loc.mpi_person_id = sp.mpi_person_id 
