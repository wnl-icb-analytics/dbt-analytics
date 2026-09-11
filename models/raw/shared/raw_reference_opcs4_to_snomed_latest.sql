{{
    config(
        description="Raw layer (SNOMED CT cross-map and terminology reference data. Sourced by NHS TRUD.). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.TERMINOLOGY.OPCS4_TO_SNOMED__LATEST \ndbt: source(''reference_trud_terminology'', ''OPCS4_TO_SNOMED__LATEST'') \nColumns:\n  OPCS4_CODE -> opcs4_code\n  OPCS4_TERM -> opcs4_term\n  OPCS_VERSION -> opcs_version\n  SNOMED_CONCEPT_ID -> snomed_concept_id\n  SNOMED_TERM -> snomed_term\n  MAP_GROUP -> map_group\n  MAP_PRIORITY -> map_priority\n  MAP_ADVICE -> map_advice\n  RELEASE_ID -> release_id"
    )
}}
select
    "OPCS4_CODE" as opcs4_code,
    "OPCS4_TERM" as opcs4_term,
    "OPCS_VERSION" as opcs_version,
    "SNOMED_CONCEPT_ID" as snomed_concept_id,
    "SNOMED_TERM" as snomed_term,
    "MAP_GROUP" as map_group,
    "MAP_PRIORITY" as map_priority,
    "MAP_ADVICE" as map_advice,
    "RELEASE_ID" as release_id
from {{ source('reference_trud_terminology', 'OPCS4_TO_SNOMED__LATEST') }}
