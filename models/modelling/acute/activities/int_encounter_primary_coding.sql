
-- ECDS
with ae_attendance_summary as (
    select visit_occurrence_id
        , sk_patient_id
        , start_date
        , chief_complaint_code as primary_complaint
        , primary_diagnosis_code_icd10 as primary_diagnosis_icd10
        , primary_diagnosis_code_snomed as primary_diagnosis_snomed
        , primary_treatment as primary_treatment_snomed
        , primary_investigation as primary_investigation_snomed
        , null as primary_procedure_opcs4
        , hrg_code as core_hrg
        , main_specialty_code as main_specialty
        , null as treatment_function
        , source
    from {{ ref('int_sus_uec_encounter') }}),

-- Admitted
admitted_spells_summary as (
    select visit_occurrence_id
        , sk_patient_id
        , start_date
        , null as primary_complaint
        , {{clean_icd10_code("primary_diagnosis_code")}} as primary_diagnosis_icd10
        , null as primary_diagnosis_snomed
        , null as primary_treatment_snomed
        , null as primary_investigation_snomed
        , primary_treatment as primary_procedure_opcs4
        , hrg_code as core_hrg
        , main_specialty_code as main_specialty
        , treatment_function_code as treatment_function
        , source
    from {{ ref('int_sus_apc_encounter') }}),

-- Outpatient
outpatient_appts_summary as (
    select visit_occurrence_id
        , sk_patient_id
        , start_date
        , null as primary_complaint
        , {{clean_icd10_code("primary_diagnosis_code")}} as primary_diagnosis_icd10
        , null as primary_diagnosis_snomed
        , null as primary_treatment_snomed
        , null as primary_investigation_snomed        
        , primary_procedure_code as primary_procedure_opcs4
        , core_hrg_code as core_hrg
        , main_specialty_code as main_specialty
        , treatment_function_code as treatment_function
        , source
    from {{ ref('int_sus_op_appointment') }}),

primary_encounters_summary as (
    select *
    from ae_attendance_summary
    
    union all

    select *
    from admitted_spells_summary
    
    union all

    select *
    from outpatient_appts_summary
)

select *
from primary_encounters_summary