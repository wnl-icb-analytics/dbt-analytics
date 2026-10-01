{{ config(materialized='table') }}

-- note: using sk_patient_id as person_id

with
    apc_diagnosis as (
        select diagnosis_id as event_id
            ,sk_patient_id
            ,visit_occurrence_id
            ,visit_occurrence_type
            ,organisation_id
            ,organisation_name
            ,date
            ,null as clinical_end_date
            ,'diagnosis' as observation_type
            ,observation_count
            ,ordered_id_array
            ,concept_code::varchar  as observation_concept_code
            ,concept_name as observation_concept_name
            ,concept_vocabulary as observation_vocabulary
        from {{ ref("int_sus_apc_diagnosis") }} apc
    ),

    apc_procedure as (
        select procedure_id as event_id
            ,sk_patient_id
            ,visit_occurrence_id
            ,visit_occurrence_type
            ,organisation_id
            ,organisation_name
            ,date
            ,null as clinical_end_date
            ,'procedure' as observation_type
            ,observation_count
            ,ordered_id_array
            ,concept_code::varchar  as observation_concept_code
            ,concept_name as observation_concept_name
            ,concept_vocabulary as observation_vocabulary
        from {{ ref("int_sus_apc_procedure") }} apc
    ),
    apc_procedure_hrg as (
        select procedure_id as event_id
            ,sk_patient_id
            ,visit_occurrence_id
            ,visit_occurrence_type
            ,organisation_id
            ,organisation_name
            ,date
            ,null as clinical_end_date
            ,'procedure' as observation_type
            ,observation_count
            ,ordered_id_array
            ,source_concept_code::varchar  as observation_concept_code
            ,concept_name as observation_concept_name
            ,concept_vocabulary as observation_vocabulary
    from {{ ref("int_sus_apc_procedure_hrg") }}
    ),
    op_diagnosis as (
        select diagnosis_id as event_id
            ,sk_patient_id
            ,visit_occurrence_id
            ,visit_occurrence_type
            ,organisation_id
            ,organisation_name
            ,date
            ,null as clinical_end_date
            ,'diagnosis' as observation_type
            ,observation_count
            ,ordered_id_array
            ,concept_code::varchar  as observation_concept_code
            ,concept_name as observation_concept_name
            ,concept_vocabulary as observation_vocabulary
        from {{ ref("int_sus_op_diagnosis") }} op
    ),
    op_procedure as (
    select procedure_id as event_id
        ,sk_patient_id
        ,visit_occurrence_id
        ,visit_occurrence_type
        ,organisation_id
        ,organisation_name
        ,date
        ,null as clinical_end_date
        ,'procedure' as observation_type
        ,observation_count
        ,ordered_id_array
        ,concept_code::varchar  as observation_concept_code
        ,concept_name as observation_concept_name
        ,concept_vocabulary as observation_vocabulary
    from {{ ref("int_sus_op_procedure") }}
),
    op_procedure_hrg as (
    select procedure_id as event_id
        ,sk_patient_id
        ,visit_occurrence_id
        ,visit_occurrence_type
        ,organisation_id
        ,organisation_name
        ,date
        ,null as clinical_end_date
        ,'procedure' as observation_type
        ,observation_count
        ,ordered_id_array
        ,source_concept_code::varchar  as observation_concept_code
        ,concept_name as observation_concept_name
        ,concept_vocabulary as observation_vocabulary
    from {{ ref("int_sus_op_procedure_hrg") }}
),

   ae_diagnosis as (
        select diagnosis_id as event_id
            ,sk_patient_id
            ,visit_occurrence_id
            ,visit_occurrence_type
            ,organisation_id
            ,organisation_name
            ,date
            ,null as clinical_end_date
            ,'diagnosis' as observation_type
            ,observation_count
            ,ordered_id_array
            ,source_concept_code::varchar  as observation_concept_code
            ,source_concept_name as observation_concept_name
            ,concept_vocabulary as observation_vocabulary
 
        from {{ ref("int_sus_uec_diagnosis") }} ae

   ),

   ae_procedure as(
    select event_id
        ,sk_patient_id
        ,visit_occurrence_id
        ,visit_occurrence_type
        ,organisation_id
        ,organisation_name
        ,date
        , null as clinical_end_date
        , observation_type
        , observation_count
        , ordered_id_array
        , snomed_code::varchar  as concept_code
        , snomed_decription as concept_name
        , 'SNOMED' as observation_vocabulary
        from {{ ref("int_sus_uec_procedure") }} ae
   ),

    all_observations as (
        select *
        from apc_diagnosis
        union all
        select *
        from apc_procedure
        union all
        select *
        from apc_procedure_hrg
        union all
        select *
        from op_diagnosis
        union all
        select *
        from op_procedure
        union all
        select * 
        from op_procedure_hrg
        union all
        select *
        from ae_diagnosis
        union all
        select *
        from ae_procedure
    )

select
    -- changed key as removing definition store join
    {{dbt_utils.generate_surrogate_key( ["event_id", "observation_concept_code", "ao.visit_occurrence_id"] )}} as record_id,
    ao.sk_patient_id,
    ao.organisation_id,
    ao.organisation_name,
    ao.visit_occurrence_id,
    ao.visit_occurrence_type,
    date,
    clinical_end_date,
    observation_type,
    observation_count,
    ordered_id_array,
    observation_concept_code,
    observation_concept_name,
    observation_vocabulary
from all_observations ao

/*
    Add restriction here to account for records in the intermediate tables with 
    observation codes in the source data that do not map to known observation codes.

    This is typically a correct code entered in a non-standard format or 
    with additional information. (i.e. "E11.3 D" instead of "E11.3")
*/
where observation_concept_code is not null
