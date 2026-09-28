{{
    config(
        description="Raw layer (Cancer waiting times data, pathway level version.). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.CWT.ActiveSystemId \ndbt: source(''cwt_pathway'', ''ActiveSystemId'') \nColumns:\n  dmicSystemId -> dmic_system_id"
    )
}}
select
    "dmicSystemId" as dmic_system_id
from {{ source('cwt_pathway', 'ActiveSystemId') }}
