{{
    config(
        description="Raw layer (SNOMED CT cross-map and terminology reference data. Sourced by NHS TRUD.). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.TERMINOLOGY.OPCS4_CODE_TERM__LATEST \ndbt: source(''reference_trud_terminology'', ''OPCS4_CODE_TERM__LATEST'') \nColumns:\n  OPCS4_CODE -> opcs4_code\n  OPCS4_TERM -> opcs4_term"
    )
}}
select
    "OPCS4_CODE" as opcs4_code,
    "OPCS4_TERM" as opcs4_term
from {{ source('reference_trud_terminology', 'OPCS4_CODE_TERM__LATEST') }}
