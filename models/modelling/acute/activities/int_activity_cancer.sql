with specialty_filters as (
    -- Combine filter conditions to avoid repetition
    select distinct visit_occurrence_id
    from {{ ref('int_sus_op_appointment')}}
    WHERE (LEFT(core_hrg_code, 2) IN ('SB', 'SC')
        OR main_specialty_code IN ('800', '370')
        OR treatment_function_code IN ('800', '370'))
    --    OR PRIMARY_DIAG BETWEEN 'C00' AND 'C97'
    --    OR PRIMARY_DIAG BETWEEN 'D00' AND 'D09')
    
    union all

    select distinct visit_occurrence_id
    from {{ ref('int_observations')}}
    where 
        (observation_vocabulary = 'HRG' and 
            (LEFT(observation_concept_code, 2) IN ('SB', 'SC')))
        OR
        (observation_vocabulary = 'ICD10' 
            and (observation_concept_code BETWEEN 'C00' AND 'C97'
            -- observation_concept_code in ('C01X','C051','C052','C07X','C080','C081','C089','C090','C091','C098','C099','C100','C101','C102','C103','C108','C109','C110','C111','C112','C113','C118','C119','C12X','C130','C131','C132','C138','C139','C320','C321','C322','C328','C329','C73X')
            OR observation_concept_code BETWEEN 'D00' AND 'D09'))
        --OR combine cancer surgery and procedure codes here 
),

cancer_encounters as (
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
inner join cancer_encounters m 
    on e.visit_occurrence_id = m.visit_occurrence_id