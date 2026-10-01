
with base_observations as (
    select * from {{ ref('int_observations') }}
    where sk_patient_id is not null
    and date between dateadd(month, -12, current_date()) and current_date()
    and observation_concept_code is not null
)

select
    sk_patient_id
    , visit_occurrence_id
    , visit_occurrence_type
    , observation_vocabulary
    , observation_type
    , count(*) as unique_code_count
from base_observations
group by
    visit_occurrence_id
    , visit_occurrence_type
    , sk_patient_id
    , observation_vocabulary
    , observation_type
