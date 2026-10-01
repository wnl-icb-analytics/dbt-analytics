{{ config(
    materialized='table',
    tags=['staging', 'olids', 'reference'],
    cluster_by=['source_concept_id']
) }}

select distinct
    source_concept_id,
    target_concept_id,
    is_primary,
    equivalence,
    source_code,
    source_display,
    source_system,
    target_code,
    target_display,
    target_system,
    equivalence_rank
from {{ ref('raw_olids_concept_map') }}
