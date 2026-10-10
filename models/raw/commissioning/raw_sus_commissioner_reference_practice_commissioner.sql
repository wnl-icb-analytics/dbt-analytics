{{
    config(
        description="Raw layer (Date-effective commissioner attribution lookups migrated from the on-premises DBA database. These tables are loaded from the SUS Consolidated Handover files.). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.SUS_COMMISSIONER_PRACTICE \ndbt: source(''sus_commissioner_reference'', ''practice_commissioner'') \nColumns:\n  SK_ORGANISATIONID_PRACTICE -> sk_organisationid_practice\n  ORGANISATIONCODE_PRACTICE -> organisationcode_practice\n  SK_ORGANISATIONID_COMMISSIONER -> sk_organisationid_commissioner\n  ORGANISATIONCODE_COMMISSIONER -> organisationcode_commissioner\n  PRACTICESTARTDATE -> practicestartdate\n  PRACTICEENDDATE -> practiceenddate\n  RELATIONSHIPSTARTDATE -> relationshipstartdate\n  RELATIONSHIPENDDATE -> relationshipenddate\n  ISACTIVE -> isactive\n  ISPROXY -> isproxy"
    )
}}
select
    "SK_ORGANISATIONID_PRACTICE" as sk_organisationid_practice,
    "ORGANISATIONCODE_PRACTICE" as organisationcode_practice,
    "SK_ORGANISATIONID_COMMISSIONER" as sk_organisationid_commissioner,
    "ORGANISATIONCODE_COMMISSIONER" as organisationcode_commissioner,
    "PRACTICESTARTDATE" as practicestartdate,
    "PRACTICEENDDATE" as practiceenddate,
    "RELATIONSHIPSTARTDATE" as relationshipstartdate,
    "RELATIONSHIPENDDATE" as relationshipenddate,
    "ISACTIVE" as isactive,
    "ISPROXY" as isproxy
from {{ source('sus_commissioner_reference', 'practice_commissioner') }}
