{{
    config(
        description="Raw layer (SNOMED CT cross-map and terminology reference data. Sourced by NHS TRUD.). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.TERMINOLOGY.SNOMED_CONCEPT_TERM__LATEST \ndbt: source(''reference_trud_terminology'', ''SNOMED_CONCEPT_TERM__LATEST'') \nColumns:\n  SNOMED_CONCEPT_ID -> snomed_concept_id\n  SNOMED_TERM -> snomed_term"
    )
}}
select
    "SNOMED_CONCEPT_ID" as snomed_concept_id,
    "SNOMED_TERM" as snomed_term
from {{ source('reference_trud_terminology', 'SNOMED_CONCEPT_TERM__LATEST') }}
