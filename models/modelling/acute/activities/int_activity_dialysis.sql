with specialty_filters as (
    -- Combine filter conditions to avoid repetition
    select visit_occurrence_id
    from {{ ref('int_observations')}}
    where 
        (observation_vocabulary = 'HRG' and 
            (observation_concept_code in ('LA08E', 'LE01A', 'LE01B', 'LE02A',  'LE02B'))) -- latter are acute injury diaysys? 
        OR
        (observation_vocabulary = 'OPCS4' and 
            (observation_concept_code LIKE 'X40%'))
   --     OR
    --    (observation_vocabulary = 'ICD10' 
    --        and (observation_concept_code IN ('N18.0', 'N18.5', 'Z49.1', 'Z49.2', 'Z99.2'))) -- remove due to concerns regarding exclusion of other renal patient appointments
),

dialysis_encounters as (
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
inner join dialysis_encounters m 
    on e.visit_occurrence_id = m.visit_occurrence_id