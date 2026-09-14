{{ config(materialized = 'table') }}

select
    snomed_code::varchar as snomed_code
    , snomed_uk_preferred_term
    , ecds_description
    , ecds_group1
    , valid_from
    , valid_to
    , dv_is_active
from {{ ref('raw_dictionary_ecds_injurydrugalcohol') }}
where snomed_code is not null
qualify row_number() over (
    partition by snomed_code
    order by dv_is_active desc nulls last, valid_to desc nulls first, valid_from desc nulls last
) = 1
