{{
    config(materialized = 'table')
}}

select
    ecds_unique_id,
    snomed_code,
    snomed_uk_preferred_term,
    ecds_description,
    ecds_group1
from {{ ref('raw_dictionary_ecds_attendancesource') }}
qualify row_number() over (
    partition by snomed_code
    order by dv_is_active desc, valid_to desc nulls first, valid_from desc
) = 1
