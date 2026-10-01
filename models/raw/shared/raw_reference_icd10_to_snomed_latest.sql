{{
    config(
        description="Raw layer (SNOMED CT cross-map and terminology reference data. Sourced by NHS TRUD.). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.TERMINOLOGY.ICD10_TO_SNOMED__LATEST \ndbt: source(''reference_trud_terminology'', ''ICD10_TO_SNOMED__LATEST'') \nColumns:\n  ICD10_CODE -> icd10_code\n  ICD10_TERM -> icd10_term\n  SNOMED_CONCEPT_ID -> snomed_concept_id\n  SNOMED_TERM -> snomed_term\n  MAP_GROUP -> map_group\n  MAP_PRIORITY -> map_priority\n  MAP_ADVICE -> map_advice\n  RELEASE_ID -> release_id"
    )
}}
select
    "ICD10_CODE" as icd10_code,
    "ICD10_TERM" as icd10_term,
    "SNOMED_CONCEPT_ID" as snomed_concept_id,
    "SNOMED_TERM" as snomed_term,
    "MAP_GROUP" as map_group,
    "MAP_PRIORITY" as map_priority,
    "MAP_ADVICE" as map_advice,
    "RELEASE_ID" as release_id
from {{ source('reference_trud_terminology', 'ICD10_TO_SNOMED__LATEST') }}
