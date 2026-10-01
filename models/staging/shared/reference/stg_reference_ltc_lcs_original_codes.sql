{{
    config(
        materialized='table'
    )
}}

select
    original_code_id,
    valueset_id,
    original_code,
    display_name,
    code_system,
    include_children,
    is_refset,
    translated_to_snomed_code,
    translated_to_display
from {{ ref('raw_reference_ltc_lcs_original_codes') }}
qualify row_number() over (partition by original_code_id order by original_code) = 1
