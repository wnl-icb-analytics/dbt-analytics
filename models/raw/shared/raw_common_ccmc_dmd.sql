{{
    config(
        description="Raw layer: Cambridge Comorbidity Score dm+d medication codelist - one row per (condition, dm+d product). Replaces AIC_DEV.BASE_CCMS_DMD_CODES. Route: connection to a London AI Centre S3 bucket of definitions, the same dependency as CCMC_SNOMED. Refresh cadence and change process are not visible to us and the exact model version is unconfirmed - flagged for review with the AI Centre.. 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.COMMON.CCMC_DMD \ndbt: source(''common'', ''CCMC_DMD'') \nColumns:\n  CONDITIONID -> conditionid\n  CONDITIONNAME -> conditionname\n  PRODUCTID -> productid\n  PRIMARYTERM -> primaryterm"
    )
}}
select
    "CONDITIONID" as conditionid,
    "CONDITIONNAME" as conditionname,
    "PRODUCTID" as productid,
    "PRIMARYTERM" as primaryterm
from {{ source('common', 'CCMC_DMD') }}
