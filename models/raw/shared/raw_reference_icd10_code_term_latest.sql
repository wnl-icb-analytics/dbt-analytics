{{
    config(
        description="Raw layer (SNOMED CT cross-map and terminology reference data. Sourced by NHS TRUD.). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.TERMINOLOGY.ICD10_CODE_TERM__LATEST \ndbt: source(''reference_trud_terminology'', ''ICD10_CODE_TERM__LATEST'') \nColumns:\n  ICD10_CODE -> icd10_code\n  ICD10_TERM -> icd10_term"
    )
}}
select
    "ICD10_CODE" as icd10_code,
    "ICD10_TERM" as icd10_term
from {{ source('reference_trud_terminology', 'ICD10_CODE_TERM__LATEST') }}
