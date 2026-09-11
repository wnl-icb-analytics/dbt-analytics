{{
    config(
        description="Raw layer (SNOMED CT cross-map and terminology reference data. Sourced by NHS TRUD.). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.TERMINOLOGY.SNOMED_TO_ICD10__LATEST \ndbt: source(''reference_trud_terminology'', ''SNOMED_TO_ICD10__LATEST'') \nColumns:\n  SNOMED_CONCEPT_ID -> snomed_concept_id\n  SNOMED_TERM -> snomed_term\n  ICD10_CODE -> icd10_code\n  ICD10_TERM -> icd10_term\n  MAP_GROUP -> map_group\n  MAP_PRIORITY -> map_priority\n  MAP_RULE -> map_rule\n  MAP_ADVICE -> map_advice\n  MAP_BLOCK -> map_block\n  EFFECTIVE_TIME -> effective_time\n  RELEASE_ID -> release_id"
    )
}}
select
    "SNOMED_CONCEPT_ID" as snomed_concept_id,
    "SNOMED_TERM" as snomed_term,
    "ICD10_CODE" as icd10_code,
    "ICD10_TERM" as icd10_term,
    "MAP_GROUP" as map_group,
    "MAP_PRIORITY" as map_priority,
    "MAP_RULE" as map_rule,
    "MAP_ADVICE" as map_advice,
    "MAP_BLOCK" as map_block,
    "EFFECTIVE_TIME" as effective_time,
    "RELEASE_ID" as release_id
from {{ source('reference_trud_terminology', 'SNOMED_TO_ICD10__LATEST') }}
