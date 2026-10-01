{{ config(materialized="table") }}

-- note: using sk_patient_id as person_id
with unbundled as(
    select primarykey_id
        , unbundled_hrg_id as problem_order
        , code
    from {{ref('stg_sus_op_appointment_commissioning_grouping_unbundled_hrg')}}
    where code is not null
),
core as (
    select
        primarykey_id, 
        0::number as problem_order, 
        appointment_commissioning_grouping_core_hrg as code
    from {{ ref('stg_sus_op_appointment')}} 
    where appointment_commissioning_grouping_core_hrg is not null
),
hrg_list as(
    select *
    from unbundled
    union 
    select *
    from core
),
flattened as(
    select primarykey_id
        , code
        , count(*) as observation_count
        , array_agg(distinct problem_order)  WITHIN GROUP (ORDER BY problem_order ASC) as ordered_id_array
    from hrg_list
    where code is not null 
    group by primarykey_id, code)

select
    {{ dbt_utils.generate_surrogate_key(["hgl.primarykey_id","hgl.code"]) }} as procedure_id,
    sa.start_date as date,
    hgl.primarykey_id as visit_occurrence_id,
    'OP_ATTENDANCE' as visit_occurrence_type,
    sa.sk_patient_id,
    sa.organisation_id,
    sa.organisation_name,  
    sa.site_id,
    sa.site_name,  -- join to reference
    case when hgl.code = sa.core_hrg_code then 'core' else 'unbundled' end as type,
    hgl.code as source_concept_code,
    hgl.observation_count,
    hgl.ordered_id_array,
    hg.hrg_description as concept_name,  -- mapped concept name from the vocabulary
    'HRG' as concept_vocabulary,
    hg.hrg_chapter,
    hg.hrg_subchapter
from flattened hgl

inner join {{ ref("int_sus_op_appointment") }} sa
    on hgl.primarykey_id = sa.visit_occurrence_id

left join {{ ref("stg_dictionary_dbo_hrg") }} hg on hgl.code = hg.hrg_code
