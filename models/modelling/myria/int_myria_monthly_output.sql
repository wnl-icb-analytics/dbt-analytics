{{
    config(
        materialized='table',
        tags=['myria_eval'])
}}
---CHECKING FOR INACTIVE, DECEASED and PALLIATIVE CARE PATIENTS in COHORT OF ELIGIBLE PATIENTS SENT TO RFL ROUGHLY EVERY 2nd TUESDAY OF THE MONTH
--All patients output - includes patients that did not match OLIDS data (is_match_olids = 0). This is because of the IS_CONFIDENTIAL flag in EMIS.
select
--add HEX
 {{ hxflake_pseudo_generation('s.patient_id') }} as hex_id, 
hospital_number,
to_varchar(
    to_timestamp_ntz(to_varchar(data_source_refresh_date)),
    'YYYY-MM-DD HH24:MI'
) as data_source_refresh_date,
--items from OLIDS historical demographics table
case when p.is_active then 1 else 0 end as is_active,
p.borough_registered as ncl_borough_registered,
nvl(p.borough_resident,'Unknown') as borough_resident,
case when p.is_deceased then 1 else 0 end as is_deceased,
p.death_date_approx,
--cross check with PDS demographics
--check is on pds system for NCL boroughs rather than wider WNL
case when pds.registered_borough in ('Barnet', 'Camden', 'Enfield', 'Haringey', 'Islington') then 1 else 0 end as is_on_pds,
case when pds.sk_patient_id is not null and p.sk_patient_id is not null then 1 else 0 end as is_match_olids,
case when pds.sk_patient_id is not null and p.sk_patient_id is null then 'Not in OLIDS' else p.inactive_reason end as inactive_reason,
--palliative care register check
case when pall.person_id is not null then 1 else 0 end as is_on_palliative_care_register,
---------------------------
gp_code,
gp_name as gp_practice_name,
local_authority,
barnet_hospital_count as bh_nel_admissions_1_year,
rfl_count as rfl_admissions_1_year,
nel_ip_admissions_last_24_months as nel_admissions_2_years,
case when s.most_recent_nel_discharge_date_bh  >= dateadd('month', -12, current_date) then 1 else 0 end as bh_discharge_last_12m,
alcohol_dependence,
atrial_fibrillation,
bronchiectasis,
cerebrovascular_disease,
chronic_kidney_disease,
chronic_liver_disease,
copd,
coronary_heart_disease,
dementia,
end_stage_renal_failure,
frailty_falls,
heart_failure,
hypertension,
liver_failure,
osteoporosis,
parkinsons_disease,
peripheral_vascular_disease,
pulmonary_heart_disease,
rheumatoid_arthritis,
severe_interstitial_lung_disease,
total_high_risk_conditions,
to_varchar(
    to_timestamp_ntz(to_varchar(most_recent_nel_admission_date)),
    'YYYY-MM-DD HH24:MI'
) as most_recent_nel_admission_date
--from MODELLING.DBT_SNAPSHOTS.FCT_PERSON_MYRIA_HIGH_RISK_PATIENTS_PUBLISHED_SNAPSHOT s
from {{ ref('fct_person_myria_high_risk_patients_published_snapshot') }} s
left join (select person_id, sk_patient_id, borough_registered, borough_resident, is_active, inactive_reason, is_deceased, death_date_approx
,row_number() over (partition by sk_patient_id order by period_sequence desc) as row_num, 
--from REPORTING.OLIDS_PERSON_DEMOGRAPHICS.DIM_PERSON_DEMOGRAPHICS_HISTORICAL
from {{ ref('dim_person_demographics_historical') }} qualify row_num = 1) p on s.patient_id = p.sk_patient_id
--additional check with PDS
--left join REPORTING.POPULATION.DIM_PERSON_DEMOGRAPHICS_BASIC pds on s.patient_id = pds.sk_patient_id
left join {{ ref('dim_person_demographics_basic') }} pds on s.patient_id = pds.sk_patient_id
--check against palliative care register
--left join REPORTING.OLIDS_DISEasE_REGISTERS.FCT_PERSON_PALLIATIVE_CARE_REGISTER  pall on pall.person_id = p.person_id
left join {{ ref('fct_person_palliative_care_register') }} pall on pall.person_id = p.person_id
--select current patient state sbt_valid_to is null
where dbt_valid_to is null

order by barnet_hospital_count desc
 
