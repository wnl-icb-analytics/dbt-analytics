{{ config(materialized='table', tags=['polypharmacy']) }}

-- NHSBSA chemical-counting chapter scope. Each SNOMED product maps once.
select
    snomed_code,
    bnf_code,
    left(bnf_code, 2) as bnf_chapter,
    bnf_name,
    left(bnf_code, 9) as bnf_chemical_code
from {{ ref('stg_reference_bnf_latest') }}
where snomed_code is not null
    and length(bnf_code) >= 9
    and left(bnf_code, 2) in ('01', '02', '03', '04', '06', '07', '08', '09', '10')
qualify row_number() over (
    partition by snomed_code
    order by bnf_code, bnf_name nulls last
) = 1
