{{
    config(
        description="Raw layer (Date-effective commissioner attribution lookups migrated from the on-premises DBA database. These tables are loaded from the SUS Consolidated Handover files.). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.SUS_COMMISSIONER_PROVIDER \ndbt: source(''sus_commissioner_reference'', ''provider_commissioner'') \nColumns:\n  PROVIDERCODE -> providercode\n  COMMISSIONERCODE -> commissionercode\n  EFFECTIVEFROM -> effectivefrom\n  EFFECTIVETO -> effectiveto"
    )
}}
select
    "PROVIDERCODE" as providercode,
    "COMMISSIONERCODE" as commissionercode,
    "EFFECTIVEFROM" as effectivefrom,
    "EFFECTIVETO" as effectiveto
from {{ source('sus_commissioner_reference', 'provider_commissioner') }}
