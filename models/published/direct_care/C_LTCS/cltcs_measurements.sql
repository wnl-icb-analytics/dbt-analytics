{{
    config(
        materialized='table',
        tags=['cltcs_secure_source'])
}}


/*
Patient data processing for CLTCS

Clinical Purpose:
- Details LTCS summary data for patients with complex needs in C-LTCS

*/
{% set measurement_cutoff = -5 %}
with inclusion_list as (
    select patient_id, area_code, olids_id
    from {{ ref('cltcs_patient_list')}}
),
hba1c_measurements as(
    select il.patient_id,
        il.area_code,
        hb.clinical_effective_date,
        hb.id as measurement_id,
        hb.hba1c_ifcc as value,
        hb.hba1c_category as category,
        case
            when hb.hba1c_category = 'Normal' then 0
            when hb.hba1c_category = 'Prediabetes' then 1
            when hb.hba1c_category = 'Diabetes - At NICE Target' then 2
            when hb.hba1c_category = 'Diabetes - Elevated (within QOF)' then 3
            when hb.hba1c_category = 'Diabetes - Above Target' then 4
            when hb.hba1c_category = 'Diabetes - High Risk' then 5
            when hb.hba1c_category = 'Diabetes - Very High Risk' then 6
            else 10
        end as colour_mapping,
        'hba1c' as measurement_type
    from {{ ref('int_hba1c_all')}} hb 
    inner join inclusion_list il on il.olids_id = hb.person_id
    where hb.clinical_effective_date between dateadd(year, {{ measurement_cutoff }}, current_date()) and current_date()
    and hb.is_valid_hba1c = true
    qualify 
        case when hb.is_ifcc then 1 when hb.is_dcct then 2 else 3 end = 
        min(case when hb.is_ifcc then 1 when hb.is_dcct then 2 else 3 end) 
            over (partition by il.patient_id, hb.clinical_effective_date)
        and row_number() over (
            partition by il.patient_id, hb.clinical_effective_date, hb.hba1c_ifcc
            order by measurement_id
        ) = 1
),
blood_pressure_measurements_systolic as(
    select il.patient_id,
        il.area_code,
        bp.clinical_effective_date,
        bp.systolic_observation_id as measurement_id,
        bp.systolic_value as value,
        case when bp.is_home_bp_event then 'HOME' when bp.is_abpm_bp_event then 'ABPM' else 'CLINIC' end as category,
        case
            when bp.is_home_bp_event then 1
            when bp.is_abpm_bp_event then 2
            else 0
        end as colour_mapping,
        'blood_pressure_systolic' as measurement_type,
    from {{ ref('int_blood_pressure_all')}} bp 
    inner join inclusion_list il on il.olids_id = bp.person_id
    where bp.clinical_effective_date between dateadd(year, {{ measurement_cutoff }}, current_date()) and current_date()
    qualify 
        row_number() over ( -- consider ranking by type of reading if which ever category is arbitrarily taken influences the clinican's decision
            partition by il.patient_id, bp.clinical_effective_date, bp.systolic_value
            order by measurement_id
        ) = 1
),
blood_pressure_measurements_diastolic as(
    select il.patient_id,
        il.area_code,
        bp.clinical_effective_date,
        bp.diastolic_observation_id as measurement_id,
        bp.diastolic_value as value,
        case when bp.is_home_bp_event then 'HOME' when bp.is_abpm_bp_event then 'ABPM' else 'CLINIC' end as category,
        case
            when bp.is_home_bp_event then 1
            when bp.is_abpm_bp_event then 2
            else 0
        end as colour_mapping,
        'blood_pressure_diastolic' as measurement_type,
    from {{ ref('int_blood_pressure_all')}} bp 
    inner join inclusion_list il on il.olids_id = bp.person_id
    where bp.clinical_effective_date between dateadd(year, {{ measurement_cutoff }}, current_date()) and current_date()
    qualify 
        row_number() over ( -- consider ranking by type of reading if which ever category is arbitrarily taken influences the clinican's decision
            partition by il.patient_id, bp.clinical_effective_date, bp.diastolic_value
            order by measurement_id
        ) = 1
),
egfr_measurements as(
    select il.patient_id,
        il.area_code,
        egfr.clinical_effective_date,
        egfr.id as measurement_id,
        egfr.egfr_value as value,
        egfr.ckd_stage as category,
        case
            when egfr.ckd_stage = 'Normal/High (≥90)' then 0
            when egfr.ckd_stage = 'Mild decrease (60-89)' then 1
            when egfr.ckd_stage = 'CKD Stage 3a (45-59)' then 2
            when egfr.ckd_stage = 'CKD Stage 3b (30-44)' then 3
            when egfr.ckd_stage = 'CKD Stage 4 (15-29)' then 4
            when egfr.ckd_stage = 'CKD Stage 5 (<15)' then 5
            else 10
        end as colour_mapping,
        'egfr' as measurement_type,
    from {{ ref('int_egfr_all')}} egfr 
    inner join inclusion_list il on il.olids_id = egfr.person_id
    where egfr.clinical_effective_date between dateadd(year, {{ measurement_cutoff }}, current_date()) and current_date()
    and egfr.is_valid_egfr = true
    qualify 
        row_number() over (
            partition by il.patient_id, egfr.clinical_effective_date, egfr.egfr_value
            order by measurement_id
        ) = 1
),
urine_acr_measurements as(
    select il.patient_id,
        il.area_code,
        acr.clinical_effective_date,
        acr.id as measurement_id,
        acr.acr_value as value,
        acr.acr_category as category,
        case
            when acr.acr_category = 'Normal (<3)' then 0
            when acr.acr_category = 'Mildly Increased (3-30)' then 1
            when acr.acr_category = 'Moderately Increased (30-300)' then 2
            when acr.acr_category = 'Severely Increased (≥300)' then 3
            else 10
        end as colour_mapping,
        'urine_acr' as measurement_type,
    from {{ ref('int_urine_acr_all')}} acr 
    inner join inclusion_list il on il.olids_id = acr.person_id
    where acr.clinical_effective_date between dateadd(year, {{ measurement_cutoff }}, current_date()) and current_date()
    and acr.is_valid_acr = true
    qualify 
        row_number() over (
            partition by il.patient_id, acr.clinical_effective_date, acr.acr_value
            order by measurement_id
        ) = 1
),
total_cholesterol_measurements as(
    select il.patient_id,
        il.area_code,
        cholesterol.clinical_effective_date,
        cholesterol.id as measurement_id,
        cholesterol.cholesterol_value as value,
        cholesterol.cholesterol_category as category,
        case
            when cholesterol.cholesterol_category = 'Desirable' then 0
            when cholesterol.cholesterol_category = 'Borderline High' then 1
            when cholesterol.cholesterol_category = 'High' then 2
            else 10
        end as colour_mapping,
        'total_cholesterol' as measurement_type,
    from {{ ref('int_cholesterol_all')}} cholesterol 
    inner join inclusion_list il on il.olids_id = cholesterol.person_id
    where cholesterol.clinical_effective_date between dateadd(year, {{ measurement_cutoff }}, current_date()) and current_date()
    and cholesterol.is_valid_cholesterol = true
    qualify 
        row_number() over (
            partition by il.patient_id, cholesterol.clinical_effective_date, cholesterol.cholesterol_value
            order by measurement_id
        ) = 1
),
ldl_cholesterol_measurements as(
    select il.patient_id,
        il.area_code,
        ldl.clinical_effective_date,
        ldl.id as measurement_id,
        ldl.cholesterol_value as value,
        ldl.ldl_cvd_target_met as category,
        case
            when ldl.ldl_cvd_target_met = 'Met' then 0
            when ldl.ldl_cvd_target_met = 'Not Met' then 1
            else 10
        end as colour_mapping,
        'ldl_cholesterol' as measurement_type,
    from {{ ref('int_cholesterol_ldl_all')}} ldl 
    inner join inclusion_list il on il.olids_id = ldl.person_id
    where ldl.clinical_effective_date between dateadd(year, {{ measurement_cutoff }}, current_date()) and current_date()
    and ldl.is_valid_cholesterol = true
    qualify 
        row_number() over (
            partition by il.patient_id, ldl.clinical_effective_date, ldl.cholesterol_value
            order by measurement_id
        ) = 1
),

eosinophil_count_measurements as (
    select il.patient_id,
        il.area_code,
        eos.clinical_effective_date,
        eos.id as measurement_id,
        eos.inferred_value as value,
        eos.eosinophil_category as category,
        case 
            when eos.eosinophil_category = 'Abnormal' then 1
            when eos.eosinophil_category = 'Eosinopenia' then 2
            when eos.eosinophil_category = 'Normal' then 3
            when eos.eosinophil_category = 'Eosinophilia' then 4
            when eos.eosinophil_category = 'Hypereosinophilia' then 5
            when eos.eosinophil_category = 'Severe Hypereosinophilia' then 6
        else 10
        end as colour_mapping,
        'eosinophil_count' as measurement_type
    from {{ ref('int_eosinophil_count')}} eos 
    inner join inclusion_list il on il.olids_id = eos.person_id
    where eos.clinical_effective_date between dateadd(year, {{ measurement_cutoff }}, current_date()) and current_date()
    qualify 
        row_number() over (
            partition by il.patient_id, eos.clinical_effective_date, eos.inferred_value
            order by measurement_id
        ) = 1
),
eosinophil_percentage_measurements as (
    select il.patient_id,
        il.area_code,
        eos_p.clinical_effective_date,
        eos_p.id as measurement_id,
        eos_p.inferred_value as value,
        eos_p.eosinophil_category as category,
        case 
            when eos_p.eosinophil_category = 'Abnormal' then 1
            when eos_p.eosinophil_category = 'Normal' then 2
            when eos_p.eosinophil_category = 'Elevated' then 3
            when eos_p.eosinophil_category = 'Very Elevated' then 4
        else 10
        end as colour_mapping,
        'eosinophil_percentage' as measurement_type
    from {{ ref('int_eosinophil_percentage')}} eos_p
    inner join inclusion_list il on il.olids_id = eos_p.person_id
    where eos_p.clinical_effective_date between dateadd(year, {{ measurement_cutoff }}, current_date()) and current_date()
    qualify 
        row_number() over (
            partition by il.patient_id, eos_p.clinical_effective_date, eos_p.inferred_value
            order by measurement_id
        ) = 1
),
complete_measurements as (
    select * from hba1c_measurements
    union all
    select * from blood_pressure_measurements_systolic
    union all
    select * from blood_pressure_measurements_diastolic
    union all
    select * from egfr_measurements
    union all
    select * from urine_acr_measurements
    union all
    select * from total_cholesterol_measurements
    union all
    select * from ldl_cholesterol_measurements
    union all
    select * from eosinophil_count_measurements
    union all
    select * from eosinophil_percentage_measurements
)

select patient_id, 
    area_code, 
    clinical_effective_date,
    measurement_type, 
    measurement_id, 
    value, 
    category,
    colour_mapping
from complete_measurements
