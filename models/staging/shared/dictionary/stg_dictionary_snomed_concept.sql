{{ config(materialized = 'table') }}

select
    sk_snomed_concept_id::varchar as snomed_code
    , preferred_term
    , is_active
    , last_updated
from {{ ref('raw_dictionary_snomed_concept') }}
where sk_snomed_concept_id is not null
qualify row_number() over (
    partition by sk_snomed_concept_id
    order by is_active desc nulls last, last_updated desc nulls last
) = 1
