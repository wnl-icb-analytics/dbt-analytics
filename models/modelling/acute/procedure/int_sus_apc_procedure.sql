{{ config(materialized="table") }}

-- note: using sk_patient_id as person_id

with
    final_opcs4_codes as (
        select primarykey_id
            , code 
            , count(*) as observation_count
            , array_agg(distinct episodes_id) WITHIN GROUP (ORDER BY episodes_id ASC) as episodes_ids
        from {{ ref("stg_sus_apc_spell_episodes_clinical_coding_procedure_opcs") }}
        where code is not null 
        group by primarykey_id, code
)

select
    {{ dbt_utils.generate_surrogate_key( ["f.primarykey_id", "f.code"])}} as procedure_id,
    se.sk_patient_id,
    se.start_date as date,
    f.primarykey_id as visit_occurrence_id,
    'APC_SPELL' as visit_occurrence_type,
    NULL as problem_order,
    se.organisation_id,
    se.organisation_name,  -- join to reference
    se.site_id,
    se.site_name,  
    f.code as source_concept_code,
    f.observation_count,
    f.episodes_ids as ordered_id_array,
    c.concept_code,
    c.concept_name,  -- mapped concept name from the vocabulary
    'OPCS4' as concept_vocabulary
from final_opcs4_codes f
left join
    {{ ref('stg_common_aicentre_vocab') }} c
    on replace(c.concept_code, '.', '') = f.code
    and c.vocabulary_id = 'OPCS4'
inner join {{ ref("int_sus_apc_encounter") }} se on se.visit_occurrence_id = f.primarykey_id
