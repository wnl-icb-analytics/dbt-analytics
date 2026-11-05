{{
    config(
        description="Raw layer (NHS organisation boundary geometry used for geographic assignment). 1:1 passthrough with cleaned column names. \nSource: Dictionary.Geo.OS_CCG_Map \ndbt: source(''dictionary_geo'', ''OS_CCG_Map'') \nColumns:\n  SK_OrganisationID -> sk_organisation_id\n  geom_text -> geom_text\n  geom -> geom\n  DateCreated -> date_created\n  DateUpdated -> date_updated"
    )
}}
select
    "SK_OrganisationID" as sk_organisation_id,
    "geom_text" as geom_text,
    "geom" as geom,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated
from {{ source('dictionary_geo', 'OS_CCG_Map') }}
