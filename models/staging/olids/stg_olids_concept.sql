{{ config(
    materialized='table',
    tags=['staging', 'olids', 'reference']
) }}

select
    -- Primary key

    -- Business columns
    system,
    code,
    display,
    is_mapped,
    use_count,


    -- New columns exposed by the 2026 OLIDS schema realignment (issue #747)
    concept_id
from {{ ref('raw_olids_concept') }}
