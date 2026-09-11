{{
    config(
        description="Raw layer (SNOMED CT cross-map and terminology reference data. Sourced by NHS TRUD.). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.TERMINOLOGY.SNOMED_TO_OPCS4__LATEST \ndbt: source(''reference_trud_terminology'', ''SNOMED_TO_OPCS4__LATEST'') \nColumns:\n  SNOMED_CONCEPT_ID -> snomed_concept_id\n  SNOMED_TERM -> snomed_term\n  OPCS4_CODE -> opcs4_code\n  OPCS4_TERM -> opcs4_term\n  OPCS_VERSION -> opcs_version\n  MAP_GROUP -> map_group\n  MAP_PRIORITY -> map_priority\n  MAP_RULE -> map_rule\n  MAP_ADVICE -> map_advice\n  MAP_BLOCK -> map_block\n  EFFECTIVE_TIME -> effective_time\n  RELEASE_ID -> release_id"
    )
}}
select
    "SNOMED_CONCEPT_ID" as snomed_concept_id,
    "SNOMED_TERM" as snomed_term,
    "OPCS4_CODE" as opcs4_code,
    "OPCS4_TERM" as opcs4_term,
    "OPCS_VERSION" as opcs_version,
    "MAP_GROUP" as map_group,
    "MAP_PRIORITY" as map_priority,
    "MAP_RULE" as map_rule,
    "MAP_ADVICE" as map_advice,
    "MAP_BLOCK" as map_block,
    "EFFECTIVE_TIME" as effective_time,
    "RELEASE_ID" as release_id
from {{ source('reference_trud_terminology', 'SNOMED_TO_OPCS4__LATEST') }}
