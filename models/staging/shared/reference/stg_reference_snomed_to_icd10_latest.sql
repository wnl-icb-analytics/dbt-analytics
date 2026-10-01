{{
    config(
        description="Staging layer: SNOMED CT to ICD-10 cross-map reference data from TRUD. Cleans and standardizes column names from the raw source."
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
