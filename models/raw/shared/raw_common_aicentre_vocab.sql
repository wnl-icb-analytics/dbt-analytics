{{
    config(
        description="Raw layer: AI Centre OMOP-style concept vocabulary (ICD-10, OPCS-4, SNOMED and others), used to attach descriptions to coded activity. Replaces AIC_DEV.BASE_ATHENA__CONCEPT. Route: UNCONFIRMED - AI Centre origin, but not established whether it refreshes from the S3 bucket or was a one-off manual load. Widest downstream reach of anything in this schema (all int_sus_* diagnosis and procedure models); confirm provenance with the AI Centre first.. 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.COMMON.AICENTRE_VOCAB \ndbt: source(''common'', ''AICENTRE_VOCAB'') \nColumns:\n  CONCEPT_ID -> concept_id\n  CONCEPT_NAME -> concept_name\n  DOMAIN_ID -> domain_id\n  VOCABULARY_ID -> vocabulary_id\n  CONCEPT_CLASS_ID -> concept_class_id\n  STANDARD_CONCEPT -> standard_concept\n  CONCEPT_CODE -> concept_code\n  VALID_START_DATE -> valid_start_date\n  VALID_END_DATE -> valid_end_date\n  INVALID_REASON -> invalid_reason"
    )
}}
select
    "CONCEPT_ID" as concept_id,
    "CONCEPT_NAME" as concept_name,
    "DOMAIN_ID" as domain_id,
    "VOCABULARY_ID" as vocabulary_id,
    "CONCEPT_CLASS_ID" as concept_class_id,
    "STANDARD_CONCEPT" as standard_concept,
    "CONCEPT_CODE" as concept_code,
    "VALID_START_DATE" as valid_start_date,
    "VALID_END_DATE" as valid_end_date,
    "INVALID_REASON" as invalid_reason
from {{ source('common', 'AICENTRE_VOCAB') }}
