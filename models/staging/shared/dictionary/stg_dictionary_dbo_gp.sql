{{ config(materialized = 'table') }}

select
    gp_code::varchar as gp_code
    , gp_name
    , start_date
    , end_date
    , last_updated
from {{ ref('raw_dictionary_dbo_gp') }}
where gp_code is not null
qualify row_number() over (
    partition by gp_code
    order by last_updated desc nulls last, end_date desc nulls first, start_date desc nulls last
) = 1
