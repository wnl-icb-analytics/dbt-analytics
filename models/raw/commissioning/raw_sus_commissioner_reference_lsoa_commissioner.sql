{{
    config(
        description="Raw layer (Date-effective commissioner attribution lookups migrated from the on-premises DBA database. These tables are loaded from the SUS Consolidated Handover files.). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.SUS_COMMISSIONER_LSOA \ndbt: source(''sus_commissioner_reference'', ''lsoa_commissioner'') \nColumns:\n  OACODE -> oacode\n  SK_OUTPUTAREAID -> sk_outputareaid\n  ORGANISATIONCODE_COMMISSIONER -> organisationcode_commissioner\n  SK_ORGANISATIONID -> sk_organisationid\n  EFFECTIVEFROM -> effectivefrom\n  EFFECTIVETO -> effectiveto"
    )
}}
select
    "OACODE" as oacode,
    "SK_OUTPUTAREAID" as sk_outputareaid,
    "ORGANISATIONCODE_COMMISSIONER" as organisationcode_commissioner,
    "SK_ORGANISATIONID" as sk_organisationid,
    "EFFECTIVEFROM" as effectivefrom,
    "EFFECTIVETO" as effectiveto
from {{ source('sus_commissioner_reference', 'lsoa_commissioner') }}
