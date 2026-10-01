{{
    config(
        materialized='table'
    )
}}

select
    exception_id,
    valueset_id,
    original_excluded_code,
    translated_to_snomed_code,
    included_in_ecl,
    translation_error
from {{ ref('raw_reference_ltc_lcs_exceptions') }}
qualify row_number() over (partition by exception_id order by original_excluded_code) = 1
