{{
    config(materialized = 'table')
}}

select
    code,
    code_category,
    lookback_years_offset,
    valproate_product_term
from {{ ref('raw_reference_valproate_prog_codes') }}
qualify row_number() over (partition by code, code_category order by valproate_product_term) = 1
