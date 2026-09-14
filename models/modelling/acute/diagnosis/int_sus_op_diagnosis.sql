{{ config(materialized="table") }}

-- note: using sk_patient_id as person_id

-- standardize the ICD codes to ensure they follow the expected format
-- `<CHAR><NUM><NUM>` or `<CHAR><NUM><NUM>.<NUM>`
with
    final_icd_codes as (
        select primarykey_id
            , code 
            , count(*) as observation_count
            , array_agg(distinct icd_id)  WITHIN GROUP (ORDER BY icd_id ASC) as icd_ids
        from {{ ref("stg_sus_op_appointment_clinical_coding_diagnosis_icd") }}
        where code is not null 
        group by primarykey_id, code
)    
select
    {{ dbt_utils.generate_surrogate_key(["f.primarykey_id", "f.code"]) }} as diagnosis_id,
    sa.sk_patient_id,
    f.primarykey_id as visit_occurrence_id,
    sa.start_date as date,
    'OP_ATTENDANCE' as visit_occurrence_type,
    sa.organisation_id,
    sa.organisation_name,  
    sa.site_id,
    sa.site_name,
    sa.start_date as activity_date,
    f.observation_count,
    f.icd_ids as ordered_id_array,
    f.code as source_concept_code,
    c.concept_code,
    c.concept_name,  -- mapped concept name from the vocabulary
    'ICD10' as concept_vocabulary
from final_icd_codes f

left join
    {{ ref('stg_common_aicentre_vocab') }} c
    on c.concept_code = f.code
    and c.vocabulary_id = 'ICD10'

inner join {{ ref("int_sus_op_appointment") }} sa
    on sa.visit_occurrence_id = f.primarykey_id
