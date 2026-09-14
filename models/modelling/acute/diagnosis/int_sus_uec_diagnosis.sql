{{ config(materialized="table") }}

-- standardize the ICD codes to ensure they follow the expected format
-- `<CHAR><NUM><NUM>` or `<CHAR><NUM><NUM>.<NUM>`
with
    diag_codes as (
        select primarykey_id
        , code
        , count(*) as observation_count
        , array_agg(distinct snomed_id)  WITHIN GROUP (ORDER BY snomed_id ASC) as snomed_ids
        from {{ ref("stg_sus_ecds_clinical_diagnoses_snomed") }} 
        where code is not null 
        group by primarykey_id, code
),
    final_icd_codes as (
        select snomed_code as code
            ,snomed_uk_preferred_term as snomed_description
            ,ecds_group1
            ,ecds_group3 
            , icd10_mapping as concept_code 
            , icd10_description as concept_name
        from {{ ref('stg_dictionary_ecds_diagnosis')}} )
    

select {{ dbt_utils.generate_surrogate_key(["f.primarykey_id", "f.code"]) }} as diagnosis_id,
    sa.sk_patient_id,
    f.primarykey_id as visit_occurrence_id,
    sa.start_date as date,
    sa.visit_occurrence_type,
    sa.organisation_id,
    sa.organisation_name,  
    sa.site_id,
    sa.site_name,
    sa.department_type,
    f.code as source_concept_code,
    f.observation_count,
    f.snomed_ids as ordered_id_array,
    d.concept_code as mapped_icd10_code,
    d.snomed_description as source_concept_name,
    'SNOMED' as concept_vocabulary,
    d.snomed_description,
    d.ecds_group1,
    d.ecds_group3

from diag_codes as f

/* Diagnosis code for infering reason */
left join {{ref('int_sus_uec_encounter')}} as sa on sa.visit_occurrence_id = f.primarykey_id

left join final_icd_codes as d on d.code = f.code
