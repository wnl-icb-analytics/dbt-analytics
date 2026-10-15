{{
    config(
        description="SNOMED CT to ICD-10 cross-map entries from the latest retained NHS TRUD release, one row per supplied mapping entry with its map group, priority, rule and advice."
    )
}}

select
    snomed_concept_id,
    snomed_term,
    icd10_code,
    icd10_term,
    map_group,
    map_priority,
    map_rule,
    map_advice,
    map_block,
    effective_time,
    release_id
from {{ ref('raw_reference_snomed_to_icd10_latest') }}
