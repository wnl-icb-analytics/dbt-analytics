-- Staging model for raw_cwt_pathway_cwt001data
-- One row represents a CWT pathway data record from the raw source.
-- Staging models may reference raw_ models directly.
{{ 
    config(materialized='view') 
}}

with raw as (
    select *
    from {{ ref('raw_cwt_pathway_cwt001data') }}
)

select
    *
from raw
