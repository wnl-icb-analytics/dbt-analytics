{{
    config(
        materialized='table',
        tags=['myria_eval'])
}}
/*JUNE 30th UPDATE FIND ALL PATIENTS ELIGIBLE FOR EVALUATIon USING USING THE DATES THAT MATCH THE RFL SUBMISSIonS. THIS FILE WILL AUTOMATICALLY UPDATE WHEN NEW SUBMISSIonS 
TO RFL ARE ADDED AND THE LATEST ENROLLMENT FILE IS ADDED from DOCCLA
4th August 2026 - onboarded days calculation changed to use discharged date if available not current date.
*/
with submission_dates as (
select distinct file_date
from {{ ref('stg_myria_rfl_submission_patients') }}
--from STAGING.MYRIA.STG_MYRIA_RFL_SUBMISSIon_PATIENTS
)
--Collect the HEX_ID from the submissions rather than regenerate
,rfl_pop as (
select distinct hospital_number, hex_id
from {{ ref('stg_myria_rfl_submission_patients') }}
--from STAGING.MYRIA.STG_MYRIA_RFL_SUBMISSION_PATIENTS
)
--JOIN BY FILE_DATE TO RFL_SUBMISSIon_PATIENTS and GET HEX_ID from THIS FILE Distinct patients (n=6395 inc June)
,eligible_patients AS ( 
select
    d.file_date as first_file_date,
    s.patient_id,
    r.hex_id,
    s.local_authority,
        date(s.most_recent_nel_admission_date) as most_recent_nel_admission_date,
        datediff('day', s.most_recent_nel_admission_date, d.file_date) days_since_nel_admission,
        s.nel_ip_admissions_last_24_months,
        s.nel_ip_admissions_last_12_months,
        s.barnet_hospital_flag,
        s.barnet_hospital_count,
        s.rfl_flag,
        s.rfl_count,
        s.total_high_risk_conditions,
        s.alcohol_dependence,
        s.atrial_fibrillation,
        s.bronchiectasis,
        s.cerebrovascular_disease,
        s.chronic_kidney_disease,
        s.chronic_liver_disease,
        s.copd,
        s.coronary_heart_disease,
        s.dementia,
        s.end_stage_renal_failure,
        s.frailty_falls,
        s.heart_failure,
        s.hypertension,
        s.liver_failure,
        s.osteoporosis,
        s.parkinsons_disease,
        s.peripheral_vascular_disease,
        s.pulmonary_heart_disease,
        s.rheumatoid_arthritis,
        s.severe_interstitial_lung_disease
from submission_dates d
join {{ ref('fct_person_myria_high_risk_patients_published_snapshot') }} s
--join modelling.dbt_snapshots.fct_person_myria_high_risk_patients_published_snapshot s
--Return rows that were active immediately before the start of the day after each FILE_DATE
    on s.dbt_valid_from < dateadd(day, 1, d.file_date)
   and (
        s.dbt_valid_to >= dateadd(day, 1, d.file_date)
        or s.dbt_valid_to is null
       )
left join rfl_pop r using (hospital_number)
--select earliest appearance of person in the snapshots
    qualify row_number() over(partition by patient_id order by file_date) = 1
)
--USING OLIDS match HISTORICAL DEMOGRAPHICS RATHER THAN CURRENT 
, demographic_information as (
        select
        e.*,
        --check against PDS as well.
        case when pds.sk_patient_id is not null then 1 else 0 end as on_pds_demog,
        case when pds.sk_patient_id is not null and p.sk_patient_id is not null then 1 else 0 end as is_match_olids,
        case when p.is_active then 1 else 0 end as is_active ,
        p.inactive_reason,
         case when p.is_deceased then 1 else 0 end as is_deceased,
        p.death_date_approx as death_date,
        p.gender, -- expected: 'male', 'female' or 'unknown'
        --Calculating age by: Taking the year difference, Subtracting 1 if the birthday hasn’t occurred yet this year- expected: 0-100 or null 
        datediff(year, p.birth_date_approx, current_date) -
        case 
        when date_part(dayofyear, p.birth_date_approx) > date_part(dayofyear, current_date) then 1 else 0 end as age_years,
       -- p.age as age_years, --compare
        p.imd_decile_25 as imd_decile,
        case when dc.person_id is not null then 1 else 0 end as is_care_home,
        p.ethnicity_category as ethnicity_group 
    from eligible_patients e
    left join {{ ref('dim_person_demographics_basic') }} pds on e.patient_id = pds.sk_patient_id
    --left join REPORTING.COMMISSIonING_REPORTING.DIM_PERSon_DEMOGRAPHICS_BASIC pds on e.patient_id = pds.sk_patient_id
    left join {{ ref('dim_person_demographics_historical') }} p on e.patient_id = p.sk_patient_id
    --left join REPORTING.OLIDS_PERSon_DEMOGRAPHICS.DIM_PERSon_DEMOGRAPHICS_HISTORICAL p on e.patient_id = p.sk_patient_id
    left join {{ ref('dim_person_care_home') }} dc on p.person_id = dc.person_id
    --left join REPORTING.OLIDS_PERSon_STATUS.DIM_PERSon_CARE_HOME dc on p.person_id = dc.person_id
    --include all historical OLIDS records as latest period_sequence - ie latest but if there is no match to OLIDS then keep in but flag
    qualify p.sk_patient_id is null or row_number() over (partition by p.sk_patient_id order by period_sequence desc) = 1  
   )
--All patients output - includes patients that did not match OLIDS data (is_match_olids = 0). This is because of the IS_ConFIDENTIAL flag in EMIS.
select 
    -- Eligibility fields
    --need patient id for matching to UEC and APC Encounter tables 
    d.patient_id,
    d.hex_id,
    d.on_pds_demog,
    d.is_match_olids,
    d.is_active,
    d.inactive_reason,
    d.death_date,
    d.first_file_date,
    d.local_authority,
    -- Myria status fields
    case when m.enrolled_date is not null then 1 else 0 end as enrolled,
    enrolled_date,
    case when m.onboarded_date is not null then 1 else 0 end as onboarded,
    onboarded_date,
    --datediff('day', onboarded_date, current_date) as days_onboarded, -- Jess changes 3rd August 26.
    case when discharged_date is not null and onboarded_date is not null
    then datediff('day', onboarded_date, discharged_date)
    else datediff('day', onboarded_date, current_date) end as days_onboarded,
    case when m.discharged_date is not null then 1 else 0 end as discharged,
    discharged_date,
    -- Demographic fields
    d.gender,
    d.age_years,
    d.imd_decile,
    d.is_care_home,
    d.ethnicity_group,
    -- High risk conditions fields
    d.most_recent_nel_admission_date,
    d.days_since_nel_admission,
    d.nel_ip_admissions_last_24_months as nel_admissions_2_years,
   -- d.nel_ip_admissions_last_12_months,
    d.barnet_hospital_count,
    d.rfl_count,
    d.total_high_risk_conditions,
    d.alcohol_dependence,
    d.atrial_fibrillation,
    d.bronchiectasis,
    d.cerebrovascular_disease,
    d.chronic_kidney_disease,
    d.chronic_liver_disease,
    d.copd,
    d.coronary_heart_disease,
    d.dementia,
    d.end_stage_renal_failure,
    d.frailty_falls,
    d.heart_failure,
    d.hypertension,
    d.liver_failure,
    d.osteoporosis,
    d.parkinsons_disease,
    d.peripheral_vascular_disease,
    d.pulmonary_heart_disease,
    d.rheumatoid_arthritis,
    d.severe_interstitial_lung_disease
from demographic_information d
-- Enrolled status from the canonical Myria table, using the latest enrolled snapshot.
-- Filtering to a single file_date keeps one row per patient (the append table holds one
-- row per patient per file) and auto-advances as new Doccla files are loaded.
left join (
    select * from {{ ref('stg_myria_enrolled_patients') }}
    --SELECT * from STAGING.MYRIA.STG_MYRIA_ENROLLED_PATIENTS
    where file_date = (select max(file_date) from {{ ref('stg_myria_enrolled_patients') }})
    --WHERE FILE_DATE = (SELECT MAX(FILE_DATE) from STAGING.MYRIA.STG_MYRIA_ENROLLED_PATIENTS)
) m on d.hex_id = m.hx_id

