{{
    config(
        description="Raw layer (NHS Talking Therapies (IAPT) dataset). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.IAPT.bridging \ndbt: source(''iapt'', ''bridging'') \nColumns:\n  Person_ID -> person_id\n  NHSNumber Pseudo -> nhs_number_pseudo"
    )
}}
select
    "Person_ID" as person_id,
    "NHSNumber Pseudo" as nhs_number_pseudo
from {{ source('iapt', 'bridging') }}
