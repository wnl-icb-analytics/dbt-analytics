{{
    config(
        materialized='table'
    )
}}

select
    concept_id,
    valueset_id,
    snomed_code,
    display,
    source,
    exclude_children
from {{ ref('raw_reference_ltc_lcs_expanded_concepts') }}
qualify row_number() over (partition by concept_id, valueset_id order by snomed_code) = 1
