{{
    config(
        description="Raw layer (Data management reference datasets). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.DATA_MANAGEMENT.CHS_SITREP_MONTHLY_OCT25_JULY26 \ndbt: source(''reference_data_management'', ''CHS_SITREP_MONTHLY_OCT25_JULY26'') \nColumns:\n  PERIOD -> period\n  ORG_NAME -> org_name\n  REGION -> region\n  ICS_NAME -> ics_name\n  SERVICE -> service\n  QUESTION -> question\n  ANSWER -> answer"
    )
}}
select
    "PERIOD" as period,
    "ORG_NAME" as org_name,
    "REGION" as region,
    "ICS_NAME" as ics_name,
    "SERVICE" as service,
    "QUESTION" as question,
    "ANSWER" as answer
from {{ source('reference_data_management', 'CHS_SITREP_MONTHLY_OCT25_JULY26') }}
