{{
    config(
        description="Raw layer (OLIDS stable layer - cleaned and filtered patient records). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.OLIDS.PATIENT_UPRN \ndbt: source(''olids'', ''PATIENT_UPRN'') \nColumns:\n  PATIENT_ADDRESS_ID -> patient_address_id\n  STATUS -> status\n  MATCHED -> matched\n  UPRN -> uprn\n  POSTCODE_QUALITY -> postcode_quality\n  QUALIFIER -> qualifier\n  CLASSIFICATION -> classification\n  ALGORITHM -> algorithm\n  MATCH_PATTERN -> match_pattern\n  PUBLISHER_ORGANISATION_CODE -> publisher_organisation_code"
    )
}}
select
    "PATIENT_ADDRESS_ID" as patient_address_id,
    "STATUS" as status,
    "MATCHED" as matched,
    "UPRN" as uprn,
    "POSTCODE_QUALITY" as postcode_quality,
    "QUALIFIER" as qualifier,
    "CLASSIFICATION" as classification,
    "ALGORITHM" as algorithm,
    "MATCH_PATTERN" as match_pattern,
    "PUBLISHER_ORGANISATION_CODE" as publisher_organisation_code
from {{ source('olids', 'PATIENT_UPRN') }}
