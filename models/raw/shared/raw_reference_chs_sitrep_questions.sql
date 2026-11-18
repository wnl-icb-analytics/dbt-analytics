{{
    config(
        description="Raw layer (Data management reference datasets). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.DATA_MANAGEMENT.CHS_SITREP_QUESTIONS \ndbt: source(''reference_data_management'', ''CHS_SITREP_QUESTIONS'') \nColumns:\n  NUMBER -> number\n  QUESTION -> question\n  ADDED_TO_COLLECTION -> added_to_collection"
    )
}}
select
    "NUMBER" as number,
    "QUESTION" as question,
    "ADDED_TO_COLLECTION" as added_to_collection
from {{ source('reference_data_management', 'CHS_SITREP_QUESTIONS') }}
