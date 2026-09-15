{{ config(materialized="table") }}

-- note: using sk_patient_id as person_id
with
    appointment_codes as (
        select primarykey_id
            , code
            , opcs_id
            , count(*) as observation_count
        from {{ ref("stg_sus_op_appointment_clinical_coding_procedure_opcs") }}
        where code is not null
        group by primarykey_id, code, opcs_id
    ),
    final_opcs4_codes as (
        select primarykey_id
            , code
            , sum(observation_count) as observation_count
            , array_agg(opcs_id) within group (order by opcs_id asc) as ordered_id_array
        from appointment_codes
        group by primarykey_id, code
)
select
    {{ dbt_utils.generate_surrogate_key(["f.primarykey_id", "f.code"]) }} as procedure_id,
    sa.start_date as date,
    f.primarykey_id as visit_occurrence_id,
    'OP_ATTENDANCE' as visit_occurrence_type,
    sa.sk_patient_id,
    sa.organisation_id,
    sa.organisation_name,  
    sa.site_id,
    sa.site_name,  -- join to reference
    f.code as source_concept_code,
    f.observation_count,
    f.ordered_id_array,
    c.concept_code,
    c.concept_name,  -- mapped concept name from the vocabulary
    'OPCS4' as concept_vocabulary
from final_opcs4_codes f

left join
     {{ ref('stg_common_aicentre_vocab') }} c
    on replace(c.concept_code, '.', '') = f.code
    and c.vocabulary_id = 'OPCS4'

left join {{ ref("int_sus_op_appointment") }} sa on sa.visit_occurrence_id = f.primarykey_id