{{ 
    config(
        materialized='table',
        tags='daily')
    }}

select 
    patient_id,
    hospital_number_most_recent_nel_bh as hospital_number,
    local_authority_sus as local_authority,
    gp_code_sus as gp_code,
    gp_name_sus as gp_name,
    local_authority_pds,
    gp_code_pds,
    gp_name_pds,
    current_age as age_at_most_recent_nel_admission,
    most_recent_nel_admission_date,
    most_recent_nel_discharge_date,
    most_recent_nel_admission_date_bh,
    most_recent_nel_discharge_date_bh,
    barnet_hospital_count,
    barnet_hospital_flag,
    RFL_ex_BH_count,
    RFL_ex_BH_flag,
    RFL_count,
    RFL_flag,
    NCLProvider_count,
    NCLProvider_flag,
    Non_NCLProvider_count,
    Non_NCLProvider_flag,
    nel_ip_admissions_last_24_months,
    nel_ip_admissions_last_12_months,
    nel_ip_admissions_last_6_months,
    heart_failure,
    copd,
    dementia,
    end_stage_renal_failure,
    severe_interstitial_lung_disease,
    parkinsons_disease,
    chronic_kidney_disease,
    liver_failure,
    alcohol_dependence,
    bronchiectasis,
    atrial_fibrillation,
    cerebrovascular_disease,
    peripheral_vascular_disease,
    pulmonary_heart_disease,
    coronary_heart_disease,
    osteoporosis,
    rheumatoid_arthritis,
    chronic_liver_disease,
    --- The following are not high risk 'conditions' but helpful flags for risk and multimorbidity
    hypertension,
    frailty_falls,
        heart_failure+
        copd+
        dementia+
        end_stage_renal_failure+
        severe_interstitial_lung_disease+
        parkinsons_disease+
        chronic_kidney_disease+
        liver_failure+
        alcohol_dependence+
        bronchiectasis+
        atrial_fibrillation+
        cerebrovascular_disease+
        peripheral_vascular_disease+
        pulmonary_heart_disease+
        coronary_heart_disease+
        osteoporosis+
        rheumatoid_arthritis+
        chronic_liver_disease as total_high_risk_conditions,
    is_on_pds,
    refresh_date as data_source_refresh_date,
    CURRENT_TIMESTAMP() as table_refresh_date
from 
    {{ ref("int_myria_conditions") }} 
where
    barnet_hospital_count >= 1
    AND local_authority_sus IN ('Barnet','Enfield')