{{
    config(
        description="Raw layer (Cancer waiting times data, pathway level version.). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.CWT.bridging \ndbt: source(''cwt_pathway'', ''bridging'') \nColumns:\n  Person_ID -> person_id\n  NHSNumber Pseudo -> nhs_number_pseudo"
    )
}}
select
    "Person_ID" as person_id,
    "NHSNumber Pseudo" as nhs_number_pseudo
from {{ source('cwt_pathway', 'bridging') }}
