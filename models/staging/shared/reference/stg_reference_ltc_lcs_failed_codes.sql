{{
    config(
        materialized='table'
    )
}}

select
    failed_code_id,
    valueset_id,
    original_code,
    display_name,
    code_system,
    reason
from {{ ref('raw_reference_ltc_lcs_failed_codes') }}
qualify row_number() over (partition by failed_code_id order by original_code) = 1
