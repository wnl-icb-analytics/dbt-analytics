{{
    config(
        description="Raw layer: Cambridge Comorbidity Score SNOMED codelist - one row per (condition, SNOMED concept). Replaces AIC_DEV.BASE_CCMS_SNOMED_CODES. Route: connection to a London AI Centre S3 bucket of definitions. Refresh cadence and change process are not visible to us and the exact model version is unconfirmed - flagged for review with the AI Centre (see \"Outstanding issues\" in docs/model_documentation/ccms_overview.md).. 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.COMMON.CCMC_SNOMED \ndbt: source(''common'', ''CCMC_SNOMED'') \nColumns:\n  CONDITIONID -> conditionid\n  CONDITIONNAME -> conditionname\n  CONCEPTID -> conceptid\n  PRIMARYTERM -> primaryterm"
    )
}}
select
    "CONDITIONID" as conditionid,
    "CONDITIONNAME" as conditionname,
    "CONCEPTID" as conceptid,
    "PRIMARYTERM" as primaryterm
from {{ source('common', 'CCMC_SNOMED') }}
