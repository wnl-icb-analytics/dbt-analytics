with specialty_filters as (
    -- Combine filter conditions to avoid repetition
    select distinct visit_occurrence_id
    from {{ ref('int_sus_op_appointment')}}
    where main_specialty_code = '501' 
       or treatment_function_code in ('501', '560')
    
    union all
    
    -- select distinct visit_occurrence_id
    -- from {{ ref('int_sus_apc_encounter')}}
    -- where main_specialty_code = '501' 
    --    or treatment_function_code in ('501', '560')
    
    -- union all
    
    select distinct visit_occurrence_id
    from {{ ref('int_observations')}}
    where observation_vocabulary = 'HRG' 
      and left(observation_concept_code, 2) = 'NZ'
),

mat_encounters as (
    select distinct visit_occurrence_id
    from specialty_filters
)

select 
    e.visit_occurrence_id
    , e.start_date
    , e.organisation_id
    , e.organisation_name
    , e.main_specialty_code
    , e.main_specialty_name
    , e.treatment_function_code
    , e.treatment_function_code_desc
    , e.core_hrg_code
    , e.core_hrg_desc
    , e.core_hrg_chapter_desc
from {{ ref('int_sus_op_appointment')}} e
inner join mat_encounters m 
    on e.visit_occurrence_id = m.visit_occurrence_id