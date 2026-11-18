-- Staging model for raw_cwt_pathway_activesystemid
-- One row represents an active system id record from the raw CWT pathway source.
-- Staging models may reference raw_ models directly.
{{ 
    config(materialized='view') 
}}

with raw as (
    select *
    from {{ ref('raw_cwt_pathway_activesystemid') }}
)

select
    *
from raw
